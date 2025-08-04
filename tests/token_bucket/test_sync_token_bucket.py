import logging
from datetime import datetime, timedelta
from functools import partial

import pytest
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from limiters import MaxSleepExceededError
from tests.conftest import SYNC_CONNECTIONS, TokenBucketConfig, sync_tokenbucket_factory

logger = logging.getLogger(__name__)

ConnectionFactory = partial[Redis] | partial[RedisCluster]


@pytest.mark.parametrize('connection', SYNC_CONNECTIONS)
async def test_sync_token_bucket(connection):
    start = datetime.now()
    config = TokenBucketConfig(refill_amount=2, refill_frequency=0.2)
    for _ in range(5):
        with sync_tokenbucket_factory(connection=connection(), config=config):
            pass

    # This has the potential of being flaky if CI is extremely slow
    assert timedelta(seconds=0) < datetime.now() - start < timedelta(seconds=1)


@pytest.mark.parametrize('connection', SYNC_CONNECTIONS)
async def test_sync_max_sleep(connection):
    e = (
        r'Scheduled to sleep \`[0-9].[0-9]+\` seconds. This exceeds the maximum accepted sleep time of \`0\.1\`'
        r' seconds.'
    )
    # This will cause the same name (key) be used for different buckets
    config = TokenBucketConfig(max_sleep=0.1)

    with sync_tokenbucket_factory(connection=connection(), config=config):
        pass

    with (
        pytest.raises(MaxSleepExceededError, match=e),
        sync_tokenbucket_factory(connection=connection(), config=config),
    ):
        pass
