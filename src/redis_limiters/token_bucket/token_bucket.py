"""
Factory classes for creating token bucket instances.
Each class will use a token bucket algorithm running locally unless a "connection"
parameter is provided for the Redis server/cluster.

You can also use the respective bucket classes directly.
 - SyncLocalTokenBucket
 - AsyncLocalTokenBucket
 - SyncRedisTokenBucket
 - AsyncRedisTokenBucket
"""

from typing import TYPE_CHECKING, Any

from redis_limiters.token_bucket.local_token_bucket import SyncLocalTokenBucket

if TYPE_CHECKING:
    from redis import Redis as SyncRedis
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster as SyncRedisCluster

    from redis_limiters.token_bucket.redis_token_bucket import AsyncRedisTokenBucket, SyncRedisTokenBucket

# Runtime availability check
try:
    import redis  # noqa: F401

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class SyncTokenBucket:
    """
    Convenience factory for creating synchronous token bucket instances.

    Automatically selects the appropriate implementation:
    - If `connection` is provided: uses Redis-based token bucket (SyncRedisTokenBucket)
    - If `connection` is None: uses local in-memory token bucket (SyncLocalTokenBucket)

    You can also import SyncRedisTokenBucket or SyncLocalTokenBucket directly.

    Examples:
        # Local in-memory bucket (no Redis required)
        >>> bucket = SyncTokenBucket(name="api", capacity=10)
        >>> with bucket:
        ...     make_api_call()

        # Redis-based bucket
        >>> from redis import Redis # or from redis.cluster import RedisCluster
        >>> # or RedisCluster(host='localhost', port=6379)
        >>> redis_conn = Redis(host='localhost', port=6379)
        >>> bucket = SyncTokenBucket(connection=redis_conn, name="api", capacity=10)
        >>> with bucket:
        ...     make_api_call()
    """

    def __new__(
        cls,
        connection: SyncRedis | SyncRedisCluster | None = None,
        **kwargs: Any,
    ) -> "SyncRedisTokenBucket | SyncLocalTokenBucket":
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(
                    "Redis support requires the 'redis' package. Install it with: pip install redis-limiters[redis]"
                )
            # Import only when needed to avoid requiring redis at module load time
            from redis_limiters.token_bucket.redis_token_bucket import SyncRedisTokenBucket

            return SyncRedisTokenBucket(connection=connection, **kwargs)
        return SyncLocalTokenBucket(**kwargs)


class AsyncTokenBucket:
    """
    Convenience factory for creating asynchronous token bucket instances.

    Automatically selects the appropriate implementation:
    - If `connection` is provided: uses Redis-based token bucket (AsyncRedisTokenBucket)
    - If `connection` is None: raises an error (local async implementation not yet available)

    For explicit control over the implementation, import and use
    AsyncRedisTokenBucket directly.

    Examples:
        # Redis-based async bucket
        >>> from redis.asyncio import Redis # or from redis.asyncio.cluster import RedisCluster
        >>> # or RedisCluster(host='localhost', port=6379)
        >>> redis_conn = Redis(host='localhost', port=6379)
        >>> bucket = AsyncTokenBucket(connection=redis_conn, name="api", capacity=10)
        >>> async with bucket:
        ...     await make_api_call()
    """

    def __new__(
        cls,
        connection: AsyncRedis | AsyncRedisCluster | None = None,
        **kwargs: Any,
    ) -> "AsyncRedisTokenBucket":
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(
                    "Redis support requires the 'redis' package. Install it with: pip install redis-limiters[redis]"
                )
            # Import only when needed to avoid requiring redis at module load time
            from redis_limiters.token_bucket.redis_token_bucket import AsyncRedisTokenBucket

            return AsyncRedisTokenBucket(connection=connection, **kwargs)
        raise NotImplementedError(
            "Local async token bucket is not yet implemented. "
            "Please provide a Redis connection or use SyncTokenBucket with SyncLocalTokenBucket."
        )
