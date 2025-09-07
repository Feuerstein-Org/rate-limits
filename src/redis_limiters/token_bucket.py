import asyncio
from datetime import datetime
from logging import getLogger
from time import sleep, time
from types import TracebackType
from typing import Annotated, ClassVar, Self, cast

from pydantic import BaseModel, Field, model_validator

from redis_limiters import MaxSleepExceededError
from redis_limiters.base import AsyncLuaScriptBase, SyncLuaScriptBase

logger = getLogger(__name__)

PositiveInt = Annotated[int, Field(gt=0)]
PositiveFloat = Annotated[float, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]


def create_redis_time_tuple() -> tuple[int, int]:
    """Create a tuple of two integers representing the current time in seconds and microseconds.

    This mimmicks the TIME command in Redis, which returns the current time in seconds and microseconds.
    See: https://redis.io/commands/time/
    """
    now = time()
    seconds_part = int(now)
    microseconds_part = int((now - seconds_part) * 1_000_000)
    return seconds_part, microseconds_part


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

        logger.info("Sleeping %s seconds (%s)", sleep_time, self.name)
        return sleep_time

    @property
    def key(self) -> str:
        return f"{{limiter}}:token-bucket:{self.name}"

    def __str__(self) -> str:
        return f"Token bucket instance for queue {self.key}"


class SyncTokenBucket(TokenBucketBase, SyncLuaScriptBase):
    script_name: ClassVar[str] = "token_bucket.lua"

    def __enter__(self) -> float:
        """Call the token bucket Lua script, receive a datetime for
        when to wake up, then sleep up until that point in time.
        """
        # Retrieve timestamp for when to wake up from Redis Lua script
        seconds, microseconds = create_redis_time_tuple()
        timestamp: int = cast(
            int,
            self.script(
                keys=[self.key],
                args=[
                    self.capacity,
                    self.refill_amount,
                    self.initial_tokens or self.capacity,
                    self.refill_frequency,
                    seconds,
                    microseconds,
                    self.expiry_seconds,
                    self.tokens_to_consume,
                ],
            ),
        )

        # Estimate sleep time
        sleep_time = self.parse_timestamp(timestamp)

        # Sleep before returning
        sleep(sleep_time)

        return sleep_time

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return


class AsyncTokenBucket(TokenBucketBase, AsyncLuaScriptBase):
    script_name: ClassVar[str] = "token_bucket.lua"

    async def __aenter__(self) -> None:
        """Call the token bucket Lua script, receive a datetime for
        when to wake up, then sleep up until that point in time.
        """
        # Retrieve timestamp for when to wake up from Redis Lua script
        seconds, microseconds = create_redis_time_tuple()
        timestamp: int = cast(
            int,
            await self.script(
                keys=[self.key],
                args=[
                    self.capacity,
                    self.refill_amount,
                    self.initial_tokens or self.capacity,
                    self.refill_frequency,
                    seconds,
                    microseconds,
                    self.expiry_seconds,
                    self.tokens_to_consume,
                ],
            ),
        )

        # Estimate sleep time
        sleep_time = self.parse_timestamp(timestamp)

        # Sleep before returning
        await asyncio.sleep(sleep_time)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return
