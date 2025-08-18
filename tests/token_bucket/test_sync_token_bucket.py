import logging
from datetime import datetime, timedelta
from functools import partial

import pytest
from redis import Redis
from redis.cluster import RedisCluster

from redis_limiters import MaxSleepExceededError
from tests.conftest import SYNC_CONNECTIONS, TokenBucketConfig, sync_tokenbucket_factory

logger = logging.getLogger(__name__)

ConnectionFactory = partial[Redis] | partial[RedisCluster]


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_sync_token_bucket(connection_factory: ConnectionFactory) -> None:
    start = datetime.now()
    config = TokenBucketConfig(capacity=1, refill_amount=1, refill_frequency=0.2)
    for _ in range(5):
        with sync_tokenbucket_factory(connection=connection_factory(), config=config):
            pass

    # This has the potential of being flaky if CI is extremely slow
    assert timedelta(seconds=0) < datetime.now() - start < timedelta(seconds=1)


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_sync_max_sleep(connection_factory: ConnectionFactory) -> None:
    # This will cause the same name (key) be used for different buckets
    config = TokenBucketConfig(max_sleep=0.1)
    # Build expected error message with actual config values
    expected_msg = (
        rf"^Rate limit exceeded for '{config.name}': would sleep "
        rf"[0-9]+\.[0-9]{{2}}s but max_sleep is {config.max_sleep}s\. "
        rf"Consider increasing capacity \({config.capacity}\) or "
        rf"refill_rate \({config.refill_amount}/{config.refill_frequency}s\)\.$"
    )

    with sync_tokenbucket_factory(connection=connection_factory(), config=config):
        pass

    with (
        pytest.raises(MaxSleepExceededError, match=expected_msg),
        sync_tokenbucket_factory(connection=connection_factory(), config=config),
    ):
        pass  # pragma: no cover
