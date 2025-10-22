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


class TokenBucketBase(BaseModel):
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
        # Parse to datetime
        wake_up_time = datetime.fromtimestamp(timestamp / 1000)

        # Establish the current time, with a very small buffer for processing time
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

    @property
    def key(self) -> str:
        return f"{{limiter}}:token-bucket:{self.name}"

    def __str__(self) -> str:
        return f"Token bucket instance for queue {self.key}"
