import asyncio
import logging
import re
from datetime import datetime
from functools import partial
from typing import Any

import pytest
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from redis_limiters import AsyncTokenBucket, MaxSleepExceededError
from tests.conftest import (
    ASYNC_CONNECTIONS,
    STANDALONE_ASYNC_CONNECTION,
    TokenBucketConfig,
    async_tokenbucket_factory,
    delta_to_seconds,
    run,
)

logger = logging.getLogger(__name__)

ConnectionFactory = partial[Redis] | partial[RedisCluster]


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
@pytest.mark.parametrize(
    "n, frequency, timeout",
    [
        (10, 0.1, 1),
        (2, 1, 2),
    ],
)
async def test_token_bucket_runtimes(
    connection_factory: ConnectionFactory, n: int, frequency: float, timeout: int
) -> None:
    connection = connection_factory()
    config = TokenBucketConfig(refill_frequency=frequency)
    # Ensure n tasks never complete in less than n/(refill_frequency * refill_amount)
    tasks = [
        asyncio.create_task(
            run(
                async_tokenbucket_factory(connection=connection, config=config),
                sleep_duration=0,
            )
        )
        for _ in range(n + 1)  # one added to account for initial capacity of 1
    ]

    before = datetime.now()
    await asyncio.gather(*tasks)
    elapsed = delta_to_seconds(datetime.now() - before)
    assert abs(timeout - elapsed) <= 0.01


@pytest.mark.parametrize("connection_factory", [STANDALONE_ASYNC_CONNECTION])
async def test_sleep_is_non_blocking(connection_factory: partial[Redis]) -> None:
    async def _sleep(sleep_duration: float) -> None:
        await asyncio.sleep(sleep_duration)

    # Create a bucket with 2 slots available (initial_tokens will default to capacity=2)
    bucket: AsyncTokenBucket = async_tokenbucket_factory(
        connection=connection_factory(),
        config=TokenBucketConfig(capacity=2, refill_amount=2),
    )

    tasks = [
        # Create four token bucket tasks and four regular sleep tasks
        # Timeline:
        # t=0: Bucket starts with 2 tokens available
        # t=0: First 2 bucket tasks acquire tokens immediately and start sleeping (1s each)
        # t=0: All 4 _sleep tasks start immediately (1s each)
        # t=0: Last 2 bucket tasks block, waiting for tokens to become available
        # t=1: First 2 bucket tasks complete, all _sleep tasks complete
        # t=1: Bucket refills with 2 tokens, last 2 bucket tasks acquire them and start sleeping
        # t=2: Last 2 bucket tasks complete
        # Total expected time: ~2 seconds
        # The interleaved _sleep tasks verify that token acquisition doesn't block other async operations
        asyncio.create_task(run(bucket, sleep_duration=1)),
        asyncio.create_task(_sleep(sleep_duration=1)),
        asyncio.create_task(run(bucket, sleep_duration=1)),
        asyncio.create_task(_sleep(sleep_duration=1)),
        asyncio.create_task(run(bucket, sleep_duration=1)),
        asyncio.create_task(_sleep(sleep_duration=1)),
        asyncio.create_task(run(bucket, sleep_duration=1)),
        asyncio.create_task(_sleep(sleep_duration=1)),
    ]

    # All tasks should complete in ~2 seconds if things are working correctly
    await asyncio.wait_for(timeout=2.2, fut=asyncio.gather(*tasks))


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
async def test_high_concurrency_token_acquisition(
    connection_factory: ConnectionFactory,
) -> None:
    """Test many concurrent tasks accessing the same bucket"""
    bucket = async_tokenbucket_factory(
        connection=connection_factory(),
        config=TokenBucketConfig(capacity=5, refill_frequency=0.1, refill_amount=5),
    )

    tasks = [asyncio.create_task(run(bucket, 0)) for _ in range(100)]
    before = datetime.now()
    await asyncio.gather(*tasks)
    elapsed = delta_to_seconds(datetime.now() - before)

    # Should take roughly (100-5)/5 * 0.1 = ~1.9 seconds
    assert elapsed >= 1.8


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
def test_repr(connection_factory: ConnectionFactory) -> None:
    config = TokenBucketConfig(name="test", capacity=1)
    tb = async_tokenbucket_factory(connection=connection_factory(), config=config)
    assert re.match(r"Token bucket instance for queue {limiter}:token-bucket:test", str(tb))


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
@pytest.mark.parametrize(
    "tokens_to_consume, refill_frequency, refill_amount, expected_requests, timeout",
    [
        # Default case: tokens_to_consume=1
        (1.0, 0.2, 1.0, 6, 0.2),
        # With tokens_to_consume=2, each request consumes 2 tokens
        # Capacity=5, so we can do 2 full requests (4 tokens)
        # With refill_frequency=0.5, refill_amount=1: we get 1 token every 0.5s
        # After initial 2 requests, we need 1 more token for 3rd request (6 total) = 0.5s wait
        (2.0, 0.5, 1.0, 3, 0.5),
        (3.0, 1.0, 1.0, 2, 1.0),
        (5.0, 0.5, 2.0, 2, 1.5),
    ],
)
async def test_token_bucket_tokens_to_consume(  # noqa: PLR0913
    connection_factory: ConnectionFactory,
    tokens_to_consume: float,
    refill_frequency: float,
    refill_amount: float,
    expected_requests: int,
    timeout: float,
) -> None:
    """Test that tokens_to_consume parameter correctly controls token consumption per request."""
    connection = connection_factory()
    config = TokenBucketConfig(
        capacity=5.0,
        refill_frequency=refill_frequency,
        refill_amount=refill_amount,
        tokens_to_consume=tokens_to_consume,
    )

    tasks = [
        asyncio.create_task(
            run(
                async_tokenbucket_factory(connection=connection, config=config),
                sleep_duration=0,
            )
        )
        for _ in range(expected_requests)
    ]

    before = datetime.now()
    await asyncio.gather(*tasks)
    elapsed = delta_to_seconds(datetime.now() - before)

    # Allow for some timing tolerance
    assert abs(timeout - elapsed) <= 0.1


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
@pytest.mark.parametrize(
    "config_params,error",
    [
        ({"name": "test"}, None),
        ({"name": None}, ValidationError),
        ({"name": 1}, ValidationError),
        ({"name": True}, ValidationError),
        ({"capacity": 2}, None),
        ({"capacity": 2.2}, None),
        ({"capacity": -1}, ValidationError),
        ({"capacity": None}, ValidationError),
        ({"capacity": "test"}, ValidationError),
        ({"refill_frequency": 2}, None),
        ({"refill_frequency": 2.2}, None),
        ({"refill_frequency": "test"}, ValidationError),
        ({"refill_frequency": None}, ValidationError),
        ({"refill_frequency": -1}, ValidationError),
        ({"refill_amount": 1}, None),
        ({"refill_amount": 0.8}, None),
        ({"refill_amount": -1}, ValidationError),
        ({"refill_amount": "test"}, ValidationError),
        ({"refill_amount": None}, ValidationError),
        ({"tokens_to_consume": 0.5}, None),
        ({"tokens_to_consume": -1}, ValidationError),
        ({"tokens_to_consume": "test"}, ValidationError),
        ({"tokens_to_consume": None}, ValidationError),
        ({"initial_tokens": 1, "capacity": 2}, None),
        ({"refill_amount": 3, "capacity": 2}, ValidationError),
        ({"initial_tokens": 3, "capacity": 2}, ValidationError),
        ({"tokens_to_consume": 3, "capacity": 4}, None),
        ({"tokens_to_consume": 3, "capacity": 2}, ValidationError),
        ({"max_sleep": 20}, None),
        ({"max_sleep": 0}, None),
        ({"max_sleep": "test"}, ValidationError),
        ({"max_sleep": None}, ValidationError),
    ],
)
def test_init_types(
    connection_factory: ConnectionFactory, config_params: dict[str, Any], error: type[ValidationError] | None
) -> None:
    if error:
        with pytest.raises(error):
            async_tokenbucket_factory(
                connection=connection_factory(),
                config=TokenBucketConfig(**config_params),
            )
    else:
        config = TokenBucketConfig(**config_params)
        async_tokenbucket_factory(connection=connection_factory(), config=config)


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
async def test_async_max_sleep(connection_factory: ConnectionFactory) -> None:
    # This will cause the same name (key) be used for different buckets
    connection = connection_factory()
    config = TokenBucketConfig(max_sleep=1.0)

    expected_msg = (
        rf"^Rate limit exceeded for '{config.name}': would sleep "
        rf"[0-9]+\.[0-9]{{2}}s but max_sleep is {config.max_sleep}s\. "
        rf"Consider increasing capacity \({config.capacity}\) or "
        rf"refill_rate \({config.refill_amount}/{config.refill_frequency}s\)\.$"
    )
    with pytest.raises(MaxSleepExceededError, match=expected_msg):
        await asyncio.gather(
            *[
                asyncio.create_task(
                    run(
                        async_tokenbucket_factory(connection=connection, config=config),
                        0,
                    )
                )
                for _ in range(10)
            ]
        )


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
@pytest.mark.parametrize(
    "initial_tokens,expected_value,expected_requests",
    [
        (None, 5, 5),  # defaults to capacity
        (2.0, 2.0, 2),  # explicit value
    ],
)
async def test_initial_tokens(
    connection_factory: ConnectionFactory, initial_tokens: float | None, expected_value: float, expected_requests: int
) -> None:
    """Test that initial_tokens defaults to capacity or uses explicit value and affects behavior."""
    config = TokenBucketConfig(capacity=5, initial_tokens=initial_tokens)
    bucket = async_tokenbucket_factory(connection=connection_factory(), config=config)

    # Test the value is set correctly
    assert bucket.initial_tokens == expected_value

    # Test behavior: expected_requests immediate + 1 after refill = ~1 second total
    start = datetime.now()

    # Assuming the capacity is 5 and initial_tokens = capacity, we are testing that 5+1 (6) requests will complete within 1 second because it
    # takes one second for an additional token to be issued.

    tasks = [asyncio.create_task(run(bucket, 0)) for _ in range(expected_requests + 1)]
    await asyncio.gather(*tasks)
    elapsed = delta_to_seconds(datetime.now() - start)

    assert 0.9 <= elapsed <= 1.1
