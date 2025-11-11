"""Common test fixtures and utilities for steindamm tests."""

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from functools import partial
from logging import Logger
from uuid import uuid4

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.client import Redis as SyncRedis
from redis.cluster import RedisCluster as SyncRedisCluster

from steindamm import (
    AsyncLocalTokenBucket,
    AsyncRedisTokenBucket,
    AsyncSemaphore,
    AsyncTokenBucket,
    SyncLocalTokenBucket,
    SyncRedisTokenBucket,
    SyncSemaphore,
    SyncTokenBucket,
)

logger: Logger = logging.getLogger(__name__)

STANDALONE_URL = "redis://127.0.0.1:6378"
CLUSTER_URL = "redis://127.0.0.1:6380"


async def initialize_async_connection(
    connection: AsyncRedis | AsyncRedisCluster | None,
) -> AsyncRedis | AsyncRedisCluster | None:
    """
    Pre-initialize async Redis connections to avoid lazy initialization overhead.

    AsyncRedisCluster has a ~26-29ms lazy initialization penalty on first use.
    This helper ensures connections are initialized before timing-sensitive operations.

    This doesn't seem to be a problem with SyncRedis or SyncRedisCluster.
    """
    if connection is not None and hasattr(connection, "initialize"):
        await connection.initialize()
    return connection


STANDALONE_SYNC_CONNECTION = partial(SyncRedis.from_url, STANDALONE_URL)
CLUSTER_SYNC_CONNECTION = partial(SyncRedisCluster.from_url, CLUSTER_URL)
STANDALONE_ASYNC_CONNECTION = partial(AsyncRedis.from_url, STANDALONE_URL)
CLUSTER_ASYNC_CONNECTION = partial(AsyncRedisCluster.from_url, CLUSTER_URL)
IN_MEMORY = lambda: None

SYNC_CONNECTIONS: list[partial[SyncRedis] | partial[SyncRedisCluster] | Callable[[], None]] = [
    STANDALONE_SYNC_CONNECTION,
    CLUSTER_SYNC_CONNECTION,
    IN_MEMORY,
]

ASYNC_CONNECTIONS: list[partial[AsyncRedis] | partial[AsyncRedisCluster] | Callable[[], None]] = [
    STANDALONE_ASYNC_CONNECTION,
    CLUSTER_ASYNC_CONNECTION,
    IN_MEMORY,
]


async def async_run(
    limiter: AsyncSemaphore | AsyncRedisTokenBucket | AsyncLocalTokenBucket, sleep_duration: float
) -> None:
    """Async: acquire the limiter and sleep for the specified duration."""
    async with limiter:
        await asyncio.sleep(sleep_duration)


def sync_run(limiter: SyncSemaphore | SyncLocalTokenBucket | SyncRedisTokenBucket, sleep_duration: float) -> None:
    """Sync: acquire the limiter and sleep for the specified duration."""
    with limiter:
        time.sleep(sleep_duration)


@dataclass
class MockTokenBucketConfig:
    """Configuration for mock token bucket instances used in tests."""

    name: str = field(default_factory=lambda: uuid4().hex[:6])
    capacity: float = 1.0
    refill_frequency: float = 1.0
    refill_amount: float = 1.0
    max_sleep: float = 0.0
    initial_tokens: float | None = None
    tokens_to_consume: float = 1.0


def sync_tokenbucket_factory(
    *, connection: SyncRedis | SyncRedisCluster | None, config: MockTokenBucketConfig
) -> SyncRedisTokenBucket | SyncLocalTokenBucket:
    """Create a SyncTokenBucket or SyncRedisTokenBucket if connection was provided."""
    return SyncTokenBucket(connection=connection, **asdict(config))


def async_tokenbucket_factory(
    *,
    connection: AsyncRedis | AsyncRedisCluster | None,
    config: MockTokenBucketConfig,
) -> AsyncRedisTokenBucket | AsyncLocalTokenBucket:
    """Create a AsyncTokenBucket or AsyncRedisTokenBucket if connection was provided."""
    return AsyncTokenBucket(connection=connection, **asdict(config))


@dataclass
class SemaphoreConfig:
    """Configuration for semaphore instances used in tests."""

    name: str = field(default_factory=lambda: uuid4().hex[:6])
    capacity: int = 1
    expiry: int = 60
    max_sleep: float = 60.0


def sync_semaphore_factory(  # noqa: D103 TODO: Fix after local semaphore is added
    *, connection: SyncRedis | SyncRedisCluster, config: SemaphoreConfig | None = None
) -> SyncSemaphore:
    if config is None:
        config = SemaphoreConfig()

    return SyncSemaphore(connection=connection, **asdict(config))


def async_semaphore_factory(  # noqa: D103 TODO: Fix after local semaphore is added
    *, connection: AsyncRedis | AsyncRedisCluster, config: SemaphoreConfig | None = None
) -> AsyncSemaphore:
    if config is None:
        config = SemaphoreConfig()

    return AsyncSemaphore(connection=connection, **asdict(config))
