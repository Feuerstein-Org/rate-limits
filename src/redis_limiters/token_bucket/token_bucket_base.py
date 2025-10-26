"""
Base class for Token Bucket rate limiters.

Defines common configuration parameters and local token bucket logic.
"""

import math
import time
from datetime import datetime
from logging import getLogger
from typing import Annotated, Self

from pydantic import BaseModel, Field, model_validator

from redis_limiters import MaxSleepExceededError

# TODO: Shouldn't this be in a more global file?
logger = getLogger(__name__)

PositiveInt = Annotated[int, Field(gt=0)]
PositiveFloat = Annotated[float, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]


def get_current_time_ms() -> int:
    """Get the current time in milliseconds."""
    return int(time.time() * 1000)


# Defaults are defined here and in Async/SyncTokenBucket to help with typehints - keep them in sync
class TokenBucketBase(BaseModel):
    """Base class for Token Bucket rate limiters."""

    name: str
    capacity: PositiveFloat = 5.0
    refill_frequency: PositiveFloat = 1.0
    initial_tokens: NonNegativeFloat | None = None
    refill_amount: PositiveFloat = 1.0
    max_sleep: NonNegativeFloat = 0.0
    expiry_seconds: PositiveInt = 30  # TODO Add tests for this
    tokens_to_consume: PositiveFloat = 1.0

    @model_validator(mode="after")
    def validate_token_bucket_config(self) -> Self:
        """Validate TokenBucketConfig parameters after initialization with Pydantic."""
        # Set initial_tokens to capacity if not explicitly provided
        if self.initial_tokens is None:
            self.initial_tokens = self.capacity

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

    def parse_timestamp(self, timestamp: int) -> float:
        """Parse a timestamp in milliseconds and determine how long to sleep."""
        # Parse to datetime
        wake_up_time = datetime.fromtimestamp(timestamp / 1000)
        now = datetime.now()

        # Return if we don't need to sleep
        if wake_up_time < now:
            return 0

        # Establish how long we should sleep
        sleep_time = (wake_up_time - now).total_seconds()

        # Raise an error if we exceed the maximum sleep setting
        if self.max_sleep != 0.0 and sleep_time > self.max_sleep:
            raise MaxSleepExceededError(
                f"Rate limit exceeded for '{self.name}': "
                f"would sleep {sleep_time:.2f}s but max_sleep is {self.max_sleep}s. "
                f"Consider increasing capacity ({self.capacity}) or refill_rate ({self.refill_amount}/{self.refill_frequency}s)."
            )

        # TODO make this debug and add more logs
        logger.info("Sleeping %s seconds (%s)", sleep_time, self.name)
        return sleep_time

    # TODO: Add whitebox tests for this method
    def execute_local_token_bucket_logic(self, buckets: dict[str, dict]) -> int:
        """
        Execute the token bucket algorithm logic locally.

        For the sync version, this method must be called while holding the bucket's lock.
        For the async version, this method is atomic.

        The local subclasses are expected to implement storage and locking around this method.
        Specifically the buckets storage is required.

        Returns:
            int: The slot timestamp in milliseconds when tokens are available.

        """
        # This method should mirror the lua script logic as closely as possible

        # Validate tokens_to_consume doesn't exceed capacity
        if self.tokens_to_consume > self.capacity:
            raise ValueError("Requested tokens exceed bucket capacity")

        if self.tokens_to_consume <= 0:
            raise ValueError("Must consume at least 1 token")

        now = get_current_time_ms()
        time_between_slots = self.refill_frequency * 1000

        # Initialize bucket state (None for new buckets)
        bucket_data = buckets.get(self.key)

        if bucket_data is None:
            # New bucket: use initial_tokens and current time as slot
            initial_tokens = self.initial_tokens if self.initial_tokens is not None else self.capacity
            tokens = min(initial_tokens, self.capacity)
            slot = now
        else:
            # Existing bucket: retrieve stored state
            slot = bucket_data["slot"]
            tokens = bucket_data["tokens"]

            # Refill tokens based on elapsed time
            slots_passed = (now - slot) // time_between_slots
            if slots_passed > 0:
                tokens = min(tokens + slots_passed * self.refill_amount, self.capacity)
                slot = now

        # If not enough tokens are available, move to the next slot(s) and refill accordingly
        if tokens < self.tokens_to_consume:
            # Calculate how many additional tokens we need
            needed_tokens = self.tokens_to_consume - tokens
            # Calculate how many slots we need to move forward to get enough tokens
            needed_slots = math.ceil(needed_tokens / self.refill_amount)
            slot += needed_slots * time_between_slots
            # Make sure we don't exceed capacity when refilling
            tokens = min(tokens + needed_slots * self.refill_amount, self.capacity)

        # Consume the requested tokens
        tokens -= self.tokens_to_consume

        # Persist updated state
        buckets[self.key] = {"slot": slot, "tokens": tokens, "last_update": time.time()}

        return int(slot)

    @property
    def key(self) -> str:
        """Key used for storing/retrieving the bucket state."""
        return f"{{limiter}}:token-bucket:{self.name}"

    def __str__(self) -> str:
        return f"Token bucket instance for queue {self.key}"
