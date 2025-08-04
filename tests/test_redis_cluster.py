import asyncio
from typing import cast

import pytest
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.client import Redis as SyncRedis
from redis.cluster import RedisCluster as SyncRedisCluster

from redis_limiters import AsyncSemaphore, AsyncTokenBucket, SyncSemaphore, SyncTokenBucket


@pytest.mark.parametrize(
    "klass,port,limiters",
    [
        (SyncRedis, 6378, [SyncSemaphore, SyncTokenBucket]),
        (SyncRedisCluster, 6380, [SyncSemaphore, SyncTokenBucket]),
        (AsyncRedis, 6378, [AsyncSemaphore, AsyncTokenBucket]),
        (AsyncRedisCluster, 6380, [AsyncSemaphore, AsyncTokenBucket]),
    ],
)
def test_redis_cluster(
    klass: SyncRedis | AsyncRedis,
    port: int,
    limiters: list[type[SyncTokenBucket | AsyncTokenBucket | SyncSemaphore | AsyncSemaphore]],
) -> None:
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
    # Ignore statements for properties that are unique in async and sync versions
    for limiter in limiters:
        limiter(
            name="test",
            capacity=99,
            max_sleep=99,
            expiry=99,  # pyright: ignore
            refill_frequency=99,  # pyright: ignore
            refill_amount=99,  # pyright: ignore
            connection=connection,
        )
