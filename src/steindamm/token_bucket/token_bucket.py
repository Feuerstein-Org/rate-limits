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

from typing import TYPE_CHECKING

from steindamm import AsyncLocalTokenBucket, SyncLocalTokenBucket

if TYPE_CHECKING:
    from redis import Redis as SyncRedis
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster as SyncRedisCluster

    from steindamm.token_bucket.redis_token_bucket import AsyncRedisTokenBucket, SyncRedisTokenBucket


# Runtime availability check
try:
    import redis  # noqa: F401

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


# Defaults are defined here and in TokenBucketBase to help with typehints - keep them in sync
class SyncTokenBucket:
    """
    Factory class for creating synchronous token bucket instances.

    Automatically selects the appropriate implementation:
    - If `connection` is provided: uses Redis-based token bucket (SyncRedisTokenBucket)
    - If `connection` is None: uses local in-memory token bucket (SyncLocalTokenBucket)

    You can also import SyncRedisTokenBucket or SyncLocalTokenBucket directly.

    Args:
        name: Unique identifier for this token bucket.
        capacity: Maximum number of tokens the bucket can hold.
        refill_frequency: Time in seconds between token refills.
        initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
        refill_amount: Number of tokens added per refill.
        max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
        expiry: Key expiry time in seconds - currently not implemented for local buckets.
        tokens_to_consume: Number of tokens to consume per operation.
        connection: Optional Redis connection (SyncRedis or SyncRedisCluster).
            If provided, uses Redis-based implementation; otherwise uses local in-memory.

    Examples:
        Local in-memory bucket (no Redis required):

        .. code-block:: python

            bucket = SyncTokenBucket(name="api", capacity=10)
            with bucket:
                make_api_call()

        Redis-based bucket:

        .. code-block:: python

            from redis import Redis  # or from redis.cluster import RedisCluster
            redis_conn = Redis(host='localhost', port=6379)
            bucket = SyncTokenBucket(connection=redis_conn, name="api", capacity=10)
            with bucket:
                make_api_call()

    """

    def __new__(  # noqa: PLR0913, D102
        cls,
        name: str,
        capacity: float = 5.0,
        refill_frequency: float = 1.0,
        initial_tokens: float | None = None,
        refill_amount: float = 1.0,
        max_sleep: float = 30.0,
        expiry: int = 60,  # TODO: Add tests for this
        tokens_to_consume: float = 1.0,
        connection: "SyncRedis | SyncRedisCluster | None" = None,
    ) -> "SyncRedisTokenBucket | SyncLocalTokenBucket":
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(
                    "Redis support requires the 'redis' package. Install it with: pip install steindamm[redis]"
                )
            # Import only when needed to avoid requiring redis at module load time
            from steindamm.token_bucket.redis_token_bucket import SyncRedisTokenBucket

            return SyncRedisTokenBucket(
                connection=connection,
                name=name,
                capacity=capacity,
                refill_frequency=refill_frequency,
                initial_tokens=initial_tokens,
                refill_amount=refill_amount,
                max_sleep=max_sleep,
                expiry=expiry,
                tokens_to_consume=tokens_to_consume,
            )
        return SyncLocalTokenBucket(
            name=name,
            capacity=capacity,
            refill_frequency=refill_frequency,
            initial_tokens=initial_tokens,
            refill_amount=refill_amount,
            max_sleep=max_sleep,
            expiry=expiry,
            tokens_to_consume=tokens_to_consume,
        )


# Defaults are defined here and in TokenBucketBase to help with typehints - keep them in sync
class AsyncTokenBucket:
    """
    Factory class for creating asynchronous token bucket instances.

    Automatically selects the appropriate implementation:
    - If `connection` is provided: uses Redis-based token bucket (AsyncRedisTokenBucket)
    - If `connection` is None: uses local in-memory token bucket (AsyncLocalTokenBucket)

    For explicit control over the implementation, import and use
    AsyncRedisTokenBucket or AsyncLocalTokenBucket directly.

    Args:
        name: Unique identifier for this token bucket.
        capacity: Maximum number of tokens the bucket can hold.
        refill_frequency: Time in seconds between token refills.
        initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
        refill_amount: Number of tokens added per refill.
        max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
        expiry: Key expiry time in seconds - currently not implemented for local buckets.
        tokens_to_consume: Number of tokens to consume per operation.
        connection: Optional async Redis connection (AsyncRedis or AsyncRedisCluster).
            If provided, uses Redis-based implementation; otherwise uses local in-memory.

    Examples:
        Local in-memory async bucket:

        .. code-block:: python

            bucket = AsyncTokenBucket(name="api", capacity=10)
            async with bucket:
                await make_api_call()

        Redis-based async bucket:

        .. code-block:: python

            from redis.asyncio import Redis
            redis_conn = Redis(host='localhost', port=6379)
            bucket = AsyncTokenBucket(connection=redis_conn, name="api", capacity=10)
            async with bucket:
                await make_api_call()

    """

    def __new__(  # noqa: PLR0913, D102
        cls,
        name: str,
        capacity: float = 5.0,
        refill_frequency: float = 1.0,
        initial_tokens: float | None = None,
        refill_amount: float = 1.0,
        max_sleep: float = 30.0,
        expiry: int = 60,  # TODO: Add tests for this
        tokens_to_consume: float = 1.0,
        connection: "AsyncRedis | AsyncRedisCluster | None" = None,
    ) -> "AsyncRedisTokenBucket | AsyncLocalTokenBucket":
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(
                    "Redis support requires the 'redis' package. Install it with: pip install steindamm[redis]"
                )
            # Import only when needed to avoid requiring redis at module load time
            from steindamm.token_bucket.redis_token_bucket import AsyncRedisTokenBucket

            return AsyncRedisTokenBucket(
                connection=connection,
                name=name,
                capacity=capacity,
                refill_frequency=refill_frequency,
                initial_tokens=initial_tokens,
                refill_amount=refill_amount,
                max_sleep=max_sleep,
                expiry=expiry,
                tokens_to_consume=tokens_to_consume,
            )
        return AsyncLocalTokenBucket(
            name=name,
            capacity=capacity,
            refill_frequency=refill_frequency,
            initial_tokens=initial_tokens,
            refill_amount=refill_amount,
            max_sleep=max_sleep,
            expiry=expiry,
            tokens_to_consume=tokens_to_consume,
        )
