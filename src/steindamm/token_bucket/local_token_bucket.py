"""Synchronous and Asynchronous local token bucket implementations."""

import asyncio
import time
from threading import Lock
from types import TracebackType
from typing import ClassVar

from steindamm.token_bucket.token_bucket_base import TokenBucketBase


class SyncLocalTokenBucket(TokenBucketBase):
    """
    Synchronous local token bucket.

    Args:
        name: Unique identifier for this token bucket.
        capacity: Maximum number of tokens the bucket can hold.
        refill_frequency: Time in seconds between token refills.
        initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
        refill_amount: Number of tokens added per refill.
        max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
        expiry: Key expiry time in seconds - currently not implemented for local buckets.
        tokens_to_consume: Number of tokens to consume per operation.

    Example:
        .. code-block:: python

           bucket = SyncLocalTokenBucket(name="api", capacity=10)
            with bucket:
                make_api_call()

    """

    # Class-level storage for bucket state (shared across instances)
    # TODO: Currently there's no cleanup of old buckets.
    # Consider adding periodic cleanup based on expiry.
    _buckets: ClassVar[dict[str, dict]] = {}
    _locks: ClassVar[dict[str, Lock]] = {}
    _main_lock: ClassVar[Lock] = Lock()

    def _get_lock(self) -> Lock:
        # This is not safe in free threaded python
        # Not acquiring main lock to improve performance in CPython with GIL
        if self.key not in self._locks:
            with self._main_lock:
                if self.key not in self._locks:
                    self._locks[self.key] = Lock()
        return self._locks[self.key]

    def __call__(self, tokens_to_consume: float | None = None) -> "SyncLocalTokenBucket":
        """
        Context manager with custom tokens_to_consume value.

        Args:
            tokens_to_consume: Number of tokens to consume. If None, uses the instance's
                tokens_to_consume value set during initialization.

        Example:
            .. code-block:: python

                bucket = SyncLocalTokenBucket(name="api", capacity=10)
                # Consume 1 token (default)
                with bucket:
                    make_small_request()
                # Consume 5 tokens
                with bucket(5):
                    make_large_request()

        """
        self._temp_tokens_to_consume = tokens_to_consume
        return self

    def __enter__(self) -> None:
        """Acquire token(s) from the token bucket and sleep until they are available."""
        # Use temporary value if set by __call__, otherwise use instance default
        tokens_needed = (
            self._temp_tokens_to_consume if self._temp_tokens_to_consume is not None else self.tokens_to_consume
        )
        # Clear temporary value
        self._temp_tokens_to_consume = None

        # Execute token bucket logic with thread safety
        with self._get_lock():
            timestamp = self.execute_local_token_bucket_logic(self._buckets, tokens_needed)

        sleep_time = self.parse_timestamp(timestamp)

        time.sleep(sleep_time)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return


class AsyncLocalTokenBucket(TokenBucketBase):
    """
    Asynchronous local token bucket.

    Args:
        name: Unique identifier for this token bucket.
        capacity: Maximum number of tokens the bucket can hold.
        refill_frequency: Time in seconds between token refills.
        initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
        refill_amount: Number of tokens added per refill.
        max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
        expiry: Key expiry time in seconds - currently not implemented for local buckets.
        tokens_to_consume: Number of tokens to consume per operation.

    Example:
        .. code-block:: python

            bucket = AsyncLocalTokenBucket(name="api", capacity=10)
            async with bucket:
                await make_api_call()

    Note: If you need to use this class from multiple threads (multiple event loops),
    consider using SyncLocalTokenBucket instead, which provides proper thread safety.

    """

    # Class-level storage for bucket state (shared across instances)
    # TODO: Currently there's no cleanup of old buckets.
    # Consider adding periodic cleanup based on expiry.
    _buckets: ClassVar[dict[str, dict]] = {}

    def __call__(self, tokens_to_consume: float | None = None) -> "AsyncLocalTokenBucket":
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
        # Use temporary value if set by __call__, otherwise use instance default
        tokens_needed = (
            self._temp_tokens_to_consume if self._temp_tokens_to_consume is not None else self.tokens_to_consume
        )
        # Clear temporary value
        self._temp_tokens_to_consume = None

        # Execute token bucket logic
        # No lock needed: asyncio is single-threaded and execute_local_token_bucket_logic
        # has no await points, making it atomic from asyncio's perspective
        timestamp = self.execute_local_token_bucket_logic(self._buckets, tokens_needed)

        sleep_time = self.parse_timestamp(timestamp)
        await asyncio.sleep(sleep_time)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return
