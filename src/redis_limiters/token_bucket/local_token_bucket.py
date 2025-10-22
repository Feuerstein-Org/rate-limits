import math
import time
from threading import Lock
from types import TracebackType
from typing import ClassVar

from redis_limiters.token_bucket.token_bucket_base import TokenBucketBase, get_current_time_ms


class SyncLocalTokenBucket(TokenBucketBase):
    """
    Thread-safe local token bucket implementation without Redis dependency.

    This class uses the exact same token bucket logic as the Redis Lua script
    but stores state in memory with thread-safe operations.
    The rate limiting is done per key which are shared across all instances of this class.
    """

    # Class-level storage for bucket state (shared across instances)
    # TODO: Currently there's no cleanup of old buckets.
    # Consider adding periodic cleanup based on expiry_seconds.
    _buckets: ClassVar[dict[str, dict]] = {}
    _locks: ClassVar[dict[str, Lock]] = {}
    _main_lock: ClassVar[Lock] = Lock()

    def _get_lock(self) -> Lock:
        if self.key not in self._locks:  # This is not safe in free threaded python
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
            timestamp = self._execute_token_bucket_logic()

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

    def _execute_token_bucket_logic(self) -> int:
        """
        Execute the token bucket algorithm logic (mirrors the Lua script).

        This method must be called while holding the bucket's lock.

        Returns:
            int: The slot timestamp in milliseconds when tokens are available.
        """
        # Validate tokens_to_consume doesn't exceed capacity
        if self.tokens_to_consume > self.capacity:
            raise ValueError("Requested tokens exceed bucket capacity")

        if self.tokens_to_consume <= 0:
            raise ValueError("Must consume at least 1 token")

        now = get_current_time_ms()
        time_between_slots = self.refill_frequency * 1000

        # Initialize bucket state (default for new buckets)
        bucket_data = self._buckets.get(self.key)

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
        self._buckets[self.key] = {"slot": slot, "tokens": tokens, "last_update": time.time()}

        return int(slot)
