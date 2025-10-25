import asyncio
import time
from threading import Lock
from types import TracebackType
from typing import ClassVar

from redis_limiters.token_bucket.token_bucket_base import TokenBucketBase


class SyncLocalTokenBucket(TokenBucketBase):
    """
    Thread-safe local token bucket implementation.

    This class uses a reservation based token bucket algorithm with bucket data stored in memory.
    The rate limiting is done per key which are shared across all instances of this class.
    """

    # Class-level storage for bucket state (shared across instances)
    # TODO: Currently there's no cleanup of old buckets.
    # Consider adding periodic cleanup based on expiry_seconds.
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
        """
        Call the token bucket logic, calculate sleep time, and sleep if needed.

        Returns:
            float: The sleep time in seconds.
        """
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
    Async-safe local token bucket implementation.

    This class uses a reservation based token bucket algorithm with bucket data stored in memory.
    The rate limiting is done per key which are shared across all instances of this class.

    Note: If you need to use this class from multiple threads (multiple event loops),
    consider using SyncLocalTokenBucket instead, which provides proper thread safety.
    """

    # Class-level storage for bucket state (shared across instances)
    # TODO: Currently there's no cleanup of old buckets.
    # Consider adding periodic cleanup based on expiry_seconds.
    _buckets: ClassVar[dict[str, dict]] = {}

    async def __aenter__(self) -> None:
        """
        Call the token bucket logic, calculate sleep time, and sleep if needed.

        Returns:
            float: The sleep time in seconds.
        """
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
