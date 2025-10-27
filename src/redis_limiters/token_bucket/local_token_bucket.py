"""Synchronous and Asynchronous local token bucket implementations."""

import asyncio
import time
from threading import Lock
from types import TracebackType
from typing import ClassVar

from redis_limiters.token_bucket.token_bucket_base import TokenBucketBase


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

    def __enter__(self) -> None:
        """Acquire token(s) from the token bucket and sleep until they are available."""
        # Execute token bucket logic with thread safety
        with self._get_lock():
            timestamp = self.execute_local_token_bucket_logic(self._buckets)

        # Parse timestamp and sleep
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

    async def __aenter__(self) -> None:
        """Acquire token(s) from the token bucket and sleep until they are available."""
        # Execute token bucket logic
        # No lock needed: asyncio is single-threaded and execute_local_token_bucket_logic
        # has no await points, making it atomic from asyncio's perspective
        timestamp = self.execute_local_token_bucket_logic(self._buckets)

        # Parse timestamp and sleep
        sleep_time = self.parse_timestamp(timestamp)
        await asyncio.sleep(sleep_time)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return
