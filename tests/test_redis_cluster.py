"""Test Redis Cluster and Standalone connections with all limiters."""

import asyncio
from typing import cast

import pytest
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.client import Redis as SyncRedis
from redis.cluster import RedisCluster as SyncRedisCluster

from redis_limiters import AsyncRedisTokenBucket, AsyncSemaphore, SyncRedisTokenBucket, SyncSemaphore


@pytest.mark.parametrize(
    "klass,port,limiters",
    [
        (SyncRedis, 6378, [SyncSemaphore, SyncRedisTokenBucket]),
        (SyncRedisCluster, 6380, [SyncSemaphore, SyncRedisTokenBucket]),
        (AsyncRedis, 6378, [AsyncSemaphore, AsyncRedisTokenBucket]),
        (AsyncRedisCluster, 6380, [AsyncSemaphore, AsyncRedisTokenBucket]),
    ],
)
def test_redis_cluster(
    klass: SyncRedis | AsyncRedis,
    port: int,
    limiters: list[type[SyncRedisTokenBucket | AsyncRedisTokenBucket | SyncSemaphore | AsyncSemaphore]],
) -> None:
    """Test that Redis Cluster and Standalone connections work with all limiters."""
    connection = klass.from_url(f"redis://127.0.0.1:{port}")
    if hasattr(connection, "__aenter__"):
        # Async connection
        async def test_async() -> None:
            async_connection = cast(AsyncRedis, connection)
            async with async_connection:
                await async_connection.info()

        asyncio.run(test_async())
    else:
        # Sync connection
        connection.info()

    # Test that all limiters can be instantiated with the connection
    # Ignore statements for properties that are unique in semaphore vs token bucket
    for limiter in limiters:
        limiter(
            name="test",
            capacity=99,
            max_sleep=99,
            expiry=99,  # pyright: ignore
            refill_frequency=99,  # pyright: ignore
            refill_amount=99,  # pyright: ignore
            connection=connection,  # type: ignore
        )
