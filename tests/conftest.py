import asyncio
import logging
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from functools import partial
from logging import Logger
from pathlib import Path
from uuid import uuid4

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.client import Redis as SyncRedis
from redis.cluster import RedisCluster as SyncRedisCluster

from redis_limiters import AsyncSemaphore, AsyncTokenBucket, SyncSemaphore, SyncTokenBucket

logger: Logger = logging.getLogger(__name__)

REPO_ROOT: Path = Path(__file__).parent.parent

STANDALONE_URL = "redis://127.0.0.1:6378"
CLUSTER_URL = "redis://127.0.0.1:6380"

STANDALONE_SYNC_CONNECTION = partial(SyncRedis.from_url, STANDALONE_URL)
CLUSTER_SYNC_CONNECTION = partial(SyncRedisCluster.from_url, CLUSTER_URL)
STANDALONE_ASYNC_CONNECTION = partial(AsyncRedis.from_url, STANDALONE_URL)
CLUSTER_ASYNC_CONNECTION = partial(AsyncRedisCluster.from_url, CLUSTER_URL)

SYNC_CONNECTIONS: list[partial[SyncRedis] | partial[SyncRedisCluster]] = [
    STANDALONE_SYNC_CONNECTION,
    CLUSTER_SYNC_CONNECTION,
]
ASYNC_CONNECTIONS: list[partial[AsyncRedis] | partial[AsyncRedisCluster]] = [
    STANDALONE_ASYNC_CONNECTION,
    CLUSTER_ASYNC_CONNECTION,
]


def delta_to_seconds(t: timedelta) -> float:
    return t.seconds + t.microseconds / 1_000_000


async def run(pt: AsyncSemaphore | AsyncTokenBucket, sleep_duration: float) -> None:
    async with pt:
        await asyncio.sleep(sleep_duration)


@dataclass
class TokenBucketConfig:
    name: str = field(default_factory=lambda: uuid4().hex[:6])
    capacity: float = 1.0
    refill_frequency: float = 1.0
    refill_amount: float = 1.0
    max_sleep: float = 0.0
    initial_tokens: float | None = None


def sync_tokenbucket_factory(*, connection: SyncRedis | SyncRedisCluster, config: TokenBucketConfig) -> SyncTokenBucket:
    return SyncTokenBucket(connection=connection, **asdict(config))


def async_tokenbucket_factory(
    *,
    connection: AsyncRedis | AsyncRedisCluster,
    config: TokenBucketConfig,
) -> AsyncTokenBucket:
    return AsyncTokenBucket(connection=connection, **asdict(config))


@dataclass
class SemaphoreConfig:
    name: str = field(default_factory=lambda: uuid4().hex[:6])
    capacity: int = 1
    expiry: int = 30
    max_sleep: float = 60.0


def sync_semaphore_factory(
    *, connection: SyncRedis | SyncRedisCluster, config: SemaphoreConfig | None = None
) -> SyncSemaphore:
    if config is None:
        config = SemaphoreConfig()

    return SyncSemaphore(connection=connection, **asdict(config))


def async_semaphore_factory(
    *, connection: AsyncRedis | AsyncRedisCluster, config: SemaphoreConfig | None = None
) -> AsyncSemaphore:
    if config is None:
        config = SemaphoreConfig()

    return AsyncSemaphore(connection=connection, **asdict(config))
