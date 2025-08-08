import logging
import threading
import time
from datetime import datetime, timedelta
from functools import partial

import pytest
from redis import Redis
from redis.cluster import RedisCluster

from redis_limiters import MaxSleepExceededError
from tests.conftest import SYNC_CONNECTIONS, SemaphoreConfig, sync_semaphore_factory

logger = logging.getLogger(__name__)

ConnectionFactory = partial[Redis] | partial[RedisCluster]


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_sync_semaphore(connection_factory: ConnectionFactory) -> None:
    start = datetime.now()
    for _ in range(5):
        with sync_semaphore_factory(connection=connection_factory()):
            time.sleep(0.2)

    # This has the potential of being flaky if CI is extremely slow
    assert timedelta(seconds=1) < datetime.now() - start < timedelta(seconds=2)


def _run(connection: Redis | RedisCluster, config: SemaphoreConfig, sleep: float) -> None:
    with sync_semaphore_factory(connection=connection, config=config):
        time.sleep(sleep)


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_sync_max_sleep(connection_factory: ConnectionFactory) -> None:
    c = connection_factory()
    config = SemaphoreConfig(max_sleep=0.1, expiry=1)
    threading.Thread(target=_run, args=(c, config, 1)).start()
    time.sleep(0.1)
    with pytest.raises(MaxSleepExceededError, match=r"Max sleep exceeded waiting for Semaphore"):
        _run(c, config, 0)
