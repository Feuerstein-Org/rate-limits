"""
Base class for Token Bucket rate limiters.

Defines common configuration parameters and local token bucket logic.
"""

import math
import time
from datetime import datetime
from logging import getLogger
from typing import Annotated, Self, cast

from pydantic import BaseModel, Field, model_validator

from steindamm.exceptions import MaxSleepExceededError, NoTokensAvailableError

# TODO: Shouldn't this be in a more global file?
logger = getLogger(__name__)

PositiveInt = Annotated[int, Field(gt=0)]
PositiveFloat = Annotated[float, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]


# Defaults are defined here and in Async/SyncTokenBucket to help with typehints - keep them in sync
class TokenBucketBase(BaseModel):
    """Base class for Token Bucket rate limiters."""

    name: str
    capacity: PositiveFloat = 5.0
    refill_frequency: NonNegativeFloat = 1.0
    initial_tokens: NonNegativeFloat | None = None
    refill_amount: NonNegativeFloat = 1.0
    max_sleep: NonNegativeFloat = 30.0
    expiry: PositiveInt = 60  # TODO Add tests for this
    tokens_to_consume: NonNegativeFloat = 1.0
    window_start_time: datetime | None = None  # Datetime in the past for window alignment
    _temp_tokens_to_consume: float | None = None  # Used internally by __call__ for context manager
    _window_start_timestamp: float | None = None  # Internal: Unix timestamp derived from window_start_time

    @model_validator(mode="after")
    def validate_token_bucket_config(self) -> Self:
        """Validate TokenBucketConfig parameters after initialization with Pydantic."""
        # Set initial_tokens to capacity if not explicitly provided
        if self.initial_tokens is None:
            self.initial_tokens = self.capacity

        # Validate and convert window_start_time to timestamp
        if self.window_start_time is not None:
            now = datetime.now()
            if self.window_start_time > now:
                raise ValueError(
                    f"Invalid token bucket '{self.name}': window_start_time ({self.window_start_time}) "
                    f"must be in the past (current time: {now})."
                )
            self._window_start_timestamp = self.window_start_time.timestamp()

        # Validate zero refill configurations
        if self.refill_frequency == 0 and self.refill_amount != 0:
            raise ValueError(
                f"Invalid token bucket '{self.name}': refill_frequency is 0 but refill_amount is {self.refill_amount}. "
                f"Either set both to 0 for a non-refilling bucket, or set refill_frequency > 0."
            )
        if self.refill_amount == 0 and self.refill_frequency != 0:
            raise ValueError(
                f"Invalid token bucket '{self.name}': refill_amount is 0 but refill_frequency is {self.refill_frequency}. "
                f"Either set both to 0 for a non-refilling bucket, or set refill_amount > 0."
            )

        if self.refill_amount > self.capacity:
            raise ValueError(
                f"Invalid token bucket '{self.name}': refill_amount ({self.refill_amount}) "
                f"cannot exceed capacity ({self.capacity}). Reduce refill_amount or increase capacity."
            )
        if self.initial_tokens > self.capacity:
            raise ValueError(
                f"Invalid token bucket '{self.name}': initial_tokens ({self.initial_tokens}) "
                f"cannot exceed capacity ({self.capacity}). Reduce initial_tokens or increase capacity."
            )
        if self.tokens_to_consume > self.capacity:
            raise ValueError(
                f"Can't consume more tokens than the bucket's capacity: {self.tokens_to_consume} > {self.capacity}"
            )
        return self

    def raise_max_sleep_exception(self, sleep_time: float) -> None:
        """Construct and raise MaxSleepExceededError with detailed message."""
        detailed_msg = (
            f"Rate limit exceeded for '{self.name}': would sleep {sleep_time:.2f}s "
            f"but max_sleep is {self.max_sleep}s. Consider increasing capacity "
            f"({self.capacity}) or refill_rate ({self.refill_amount}/{self.refill_frequency}s)."
        )
        raise MaxSleepExceededError(detailed_msg)

    def parse_timestamp(self, timestamp: float) -> float:
        """Parse timestamp and calculate sleep time."""
        wake_up_time = datetime.fromtimestamp(timestamp / 1000)
        now = datetime.now()

        if wake_up_time < now:
            return 0.0

        sleep_time = (wake_up_time - now).total_seconds()

        # TODO make this debug and add more logs
        logger.info("Sleeping %s seconds (%s)", sleep_time, self.name)
        return sleep_time

    # TODO: Add whitebox tests for this method
    # This mimics the logic in the lua script as closely as possible
    def execute_local_token_bucket_logic(  # noqa: C901, PLR0912
        self, buckets: dict[str, dict], tokens_to_consume: float | None = None
    ) -> float:
        """
        Execute the token bucket algorithm logic locally.

        For the sync version, this method must be called while holding the bucket's lock.
        For the async version, this method is atomic.

        The local subclasses are expected to implement storage and locking around this method.
        Specifically the buckets storage is required.

        Args:
            buckets: Storage dictionary for bucket states.
            tokens_to_consume: Number of tokens to consume. If None, uses self.tokens_to_consume.

        Returns:
            float: The slot timestamp in milliseconds when tokens are available.

        """
        # This method should mirror the lua script logic as closely as possible

        # Use provided tokens_to_consume or fall back to instance variable
        tokens_needed = tokens_to_consume if tokens_to_consume is not None else self.tokens_to_consume

        # Validate tokens_to_consume doesn't exceed capacity
        if tokens_needed > self.capacity:
            raise ValueError("Requested tokens exceed bucket capacity")

        if tokens_needed < 0:
            raise ValueError("Can't consume negative tokens")

        now = time.time() * 1000

        # Handle non-refilling buckets (refill_amount or refill_frequency is 0)
        is_non_refilling = self.refill_amount == 0 or self.refill_frequency == 0
        refill_frequency_ms = self.refill_frequency * 1000

        # Initialize bucket state (None for new buckets)
        bucket_data = buckets.get(self.key)

        if bucket_data is None:
            # New bucket: use initial_tokens and aligned slot if window_start_time is set
            initial_tokens = cast(float, self.initial_tokens)  # Set in config validator
            tokens = min(initial_tokens, self.capacity)
            if self._window_start_timestamp is not None:
                # Align slot to window_start_time
                slot_start_ms = self._window_start_timestamp * 1000
                slots_since_start = (now - slot_start_ms) // refill_frequency_ms
                slot = slot_start_ms + slots_since_start * refill_frequency_ms
            else:
                slot = now
        else:
            # Existing bucket: retrieve stored state
            slot = bucket_data["slot"]
            tokens = bucket_data["tokens"]

            # Refill tokens based on elapsed time (skip for non-refilling buckets)
            if not is_non_refilling:
                slots_passed = (now - slot) // refill_frequency_ms
                if slots_passed > 0:
                    tokens = min(tokens + slots_passed * self.refill_amount, self.capacity)
                    if self._window_start_timestamp is not None:
                        # Align slot to window_start_time
                        slot_start_ms = self._window_start_timestamp * 1000
                        slots_since_start = (now - slot_start_ms) // refill_frequency_ms
                        slot = slot_start_ms + slots_since_start * refill_frequency_ms
                    else:
                        slot = now

        # If not enough tokens are available, move to the next slot(s) and refill accordingly
        if tokens < tokens_needed:
            if is_non_refilling:
                # Non-refilling bucket has run out of tokens - raise exception
                raise NoTokensAvailableError(
                    f"Token bucket '{self.name}' has run out of tokens. "
                    f"Available: {tokens}, Requested: {tokens_needed}. "
                    f"This is a non-refilling bucket (refill_amount={self.refill_amount}, "
                    f"refill_frequency={self.refill_frequency})."
                )
            # Calculate how many additional tokens we need
            needed_tokens = tokens_needed - tokens
            # Calculate how many slots we need to move forward to get enough tokens
            needed_slots = math.ceil(needed_tokens / self.refill_amount)
            slot += needed_slots * refill_frequency_ms
            # Make sure we don't exceed capacity when refilling
            tokens = min(tokens + needed_slots * self.refill_amount, self.capacity)

        # Validate max_sleep BEFORE consuming tokens
        required_sleep = max(0.0, slot - now) / 1000  # Convert to seconds
        if self.max_sleep > 0 and required_sleep > self.max_sleep:
            self.raise_max_sleep_exception(required_sleep)

        # Consume the requested tokens
        tokens -= tokens_needed

        # Persist updated state
        buckets[self.key] = {"slot": slot, "tokens": tokens, "last_update": time.time()}

        return slot

    @property
    def key(self) -> str:
        """Key used for storing/retrieving the bucket state."""
        return f"{{limiter}}:token-bucket:{self.name}"

    def __str__(self) -> str:
        return f"Token bucket instance for queue {self.key}"
