import asyncio
import time
from types import TracebackType
from typing import ClassVar, cast

from redis_limiters.base import AsyncLuaScriptBase, SyncLuaScriptBase
from redis_limiters.token_bucket.token_bucket_base import TokenBucketBase, get_current_time_ms


class SyncRedisTokenBucket(TokenBucketBase, SyncLuaScriptBase):
    script_name: ClassVar[str] = "token_bucket/token_bucket.lua"

    def __enter__(self) -> float:
        """
        Call the token bucket Lua script, receive a datetime for
        when to wake up, then sleep up until that point in time.
        """
        # Retrieve timestamp for when to wake up from Redis Lua script
        milliseconds = get_current_time_ms()
        timestamp: int = cast(
            int,
            self.script(
                keys=[self.key],
                args=[
                    self.capacity,
                    self.refill_amount,
                    self.initial_tokens or self.capacity,
                    self.refill_frequency,
                    milliseconds,
                    self.expiry_seconds,
                    self.tokens_to_consume,
                ],
            ),
        )

        # Estimate sleep time
        sleep_time = self.parse_timestamp(timestamp)

        # Sleep before returning
        time.sleep(sleep_time)

        return sleep_time

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return


class AsyncRedisTokenBucket(TokenBucketBase, AsyncLuaScriptBase):
    script_name: ClassVar[str] = "token_bucket/token_bucket.lua"

    async def __aenter__(self) -> None:
        """
        Call the token bucket Lua script, receive a datetime for
        when to wake up, then sleep up until that point in time.
        """
        # Retrieve timestamp for when to wake up from Redis Lua script
        milliseconds = get_current_time_ms()
        timestamp: int = cast(
            int,
            await self.script(
                keys=[self.key],
                args=[
                    self.capacity,
                    self.refill_amount,
                    self.initial_tokens or self.capacity,
                    self.refill_frequency,
                    milliseconds,
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
