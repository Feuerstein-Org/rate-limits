import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial

import pytest
from redis import Redis
from redis.cluster import RedisCluster

from redis_limiters import MaxSleepExceededError
from tests.conftest import SYNC_CONNECTIONS, TokenBucketConfig, delta_to_seconds, sync_run, sync_tokenbucket_factory

logger = logging.getLogger(__name__)

ConnectionFactory = partial[Redis] | partial[RedisCluster] | Callable[[], None]


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
@pytest.mark.parametrize(
    "n, frequency, timeout",
    [
        (10, 0.1, 1),
        (2, 1, 2),
    ],
)
def test_token_bucket_runtimes(connection_factory: ConnectionFactory, n: int, frequency: float, timeout: int) -> None:
    connection = connection_factory()
    config = TokenBucketConfig(refill_frequency=frequency)
    bucket = sync_tokenbucket_factory(connection=connection, config=config)

    # Ensure n tasks never complete in less than n/(refill_frequency * refill_amount)
    before = datetime.now()

    with ThreadPoolExecutor(max_workers=n + 1) as executor:
        futures = [
            executor.submit(sync_run, bucket, 0)
            for _ in range(n + 1)  # one added to account for initial capacity of 1
        ]
        # Wait for all threads to complete
        for future in futures:
            future.result()

    elapsed = delta_to_seconds(datetime.now() - before)
    assert abs(timeout - elapsed) <= 0.01


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

    bucket = sync_tokenbucket_factory(connection=connection_factory(), config=config)

    with bucket:
        pass

    with (
        pytest.raises(MaxSleepExceededError, match=expected_msg),
        bucket,
    ):
        pass  # pragma: no cover
