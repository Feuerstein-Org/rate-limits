"""Synchronous and Asynchronous Redis-backed (Standalone or Cluster) token bucket implementations."""

import asyncio
import time
from types import TracebackType
from typing import ClassVar, cast

from steindamm.base import AsyncLuaScriptBase, SyncLuaScriptBase
from steindamm.token_bucket.token_bucket_base import TokenBucketBase, get_current_time_ms


class SyncRedisTokenBucket(TokenBucketBase, SyncLuaScriptBase):
    """
    Synchronous Redis-backed (Standalone or Cluster) token bucket.

    Args:
        name: Unique identifier for this token bucket.
        connection: Redis connection (SyncRedis or SyncRedisCluster).
        capacity: Maximum number of tokens the bucket can hold.
        refill_frequency: Time in seconds between token refills.
        initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
        refill_amount: Number of tokens added per refill.
        max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
        expiry: Key expiry time in seconds.
        tokens_to_consume: Number of tokens to consume per operation.

    Example:
        .. code-block:: python

            from redis import Redis  # or from redis.cluster import RedisCluster
            redis_conn = Redis(host='localhost', port=6379)
            bucket = SyncRedisTokenBucket(connection=redis_conn, name="api", capacity=10)
            with bucket:
                make_api_call()

    """

    script_name: ClassVar[str] = "token_bucket/token_bucket.lua"

    def __call__(self, tokens_to_consume: float | None = None) -> "SyncRedisTokenBucket":
        """
        Context manager with custom tokens_to_consume value.

        Args:
            tokens_to_consume: Number of tokens to consume. If None, uses the instance's
                tokens_to_consume value set during initialization.

        Example:
            .. code-block:: python

                bucket = SyncRedisTokenBucket(connection=redis_conn, name="api", capacity=10)
                # Consume 1 token (default)
                with bucket:
                    make_small_request()
                # Consume 5 tokens
                with bucket(5):
                    make_large_request()

        """
        self._temp_tokens_to_consume = tokens_to_consume
        return self

    def __enter__(self) -> float:
        """Acquire token(s) from the token bucket and sleep until they are available."""
        # Retrieve timestamp for when to wake up from Redis Lua script
        milliseconds = get_current_time_ms()
        # Use temporary value if set by __call__, otherwise use instance default
        tokens_needed = (
            self._temp_tokens_to_consume if self._temp_tokens_to_consume is not None else self.tokens_to_consume
        )
        # Clear temporary value
        self._temp_tokens_to_consume = None

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
                    self.expiry,
                    tokens_needed,
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
    """
    Asynchronous Redis-backed (Standalone or Cluster) token bucket.

    Args:
        name: Unique identifier for this token bucket.
        connection: Redis connection (AsyncRedis or AsyncRedisCluster).
        capacity: Maximum number of tokens the bucket can hold.
        refill_frequency: Time in seconds between token refills.
        initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
        refill_amount: Number of tokens added per refill.
        max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
        expiry: Key expiry time in seconds.
        tokens_to_consume: Number of tokens to consume per operation.

    Example:
        .. code-block:: python

            from redis.asyncio import Redis  # or from redis.asyncio.cluster import RedisCluster
            redis_conn = Redis(host='localhost', port=6379)
            bucket = AsyncRedisTokenBucket(connection=redis_conn, name="api", capacity=10)
            async with bucket:
                await make_api_call()

    """

    script_name: ClassVar[str] = "token_bucket/token_bucket.lua"

    def __call__(self, tokens_to_consume: float | None = None) -> "AsyncRedisTokenBucket":
        """
        Context manager with custom tokens_to_consume value.

        Args:
            tokens_to_consume: Number of tokens to consume. If None, uses the instance's
                tokens_to_consume value set during initialization.

        Example:
            .. code-block:: python

                bucket = AsyncRedisTokenBucket(connection=redis_conn, name="api", capacity=10)
                # Consume 1 token (default)
                async with bucket:
                    await make_small_request()
                # Consume 5 tokens
                async with bucket(5):
                    await make_large_request()

        """
        self._temp_tokens_to_consume = tokens_to_consume
        return self

    async def __aenter__(self) -> None:
        """Acquire token(s) from the token bucket and sleep until they are available."""
        # Retrieve timestamp for when to wake up from Redis Lua script
        milliseconds = get_current_time_ms()
        # Use temporary value if set by __call__, otherwise use instance default
        tokens_needed = (
            self._temp_tokens_to_consume if self._temp_tokens_to_consume is not None else self.tokens_to_consume
        )
        # Clear temporary value
        self._temp_tokens_to_consume = None

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
                    self.expiry,
                    tokens_needed,
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
