"""
Various token bucket and semaphore implementations using a Redis or local backend.

Use SyncTokenBucket or AsyncTokenBucket to automatically select between Redis-based
and local in-memory implementations based on whether a Redis connection is provided.

For explicit control over the implementation, import and use
SyncRedisTokenBucket, AsyncRedisTokenBucket, SyncLocalTokenBucket, or AsyncLocalTokenBucket directly.
"""

# TODO: Add local semaphore implementation and update docs accordingly
from redis_limiters.exceptions import MaxSleepExceededError
from redis_limiters.semaphore import AsyncSemaphore, SyncSemaphore
from redis_limiters.token_bucket.local_token_bucket import AsyncLocalTokenBucket, SyncLocalTokenBucket
from redis_limiters.token_bucket.redis_token_bucket import AsyncRedisTokenBucket, SyncRedisTokenBucket
from redis_limiters.token_bucket.token_bucket import AsyncTokenBucket, SyncTokenBucket

__all__ = (
    "AsyncLocalTokenBucket",
    "AsyncRedisTokenBucket",
    "AsyncSemaphore",
    "AsyncTokenBucket",
    "MaxSleepExceededError",
    "SyncLocalTokenBucket",
    "SyncRedisTokenBucket",
    "SyncSemaphore",
    "SyncTokenBucket",
)
