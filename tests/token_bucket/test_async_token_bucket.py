"""Test asynchronous token bucket implementation."""

import asyncio
import logging
import re
import time
from functools import partial

import pytest
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from steindamm import MaxSleepExceededError
from tests.conftest import (
    ASYNC_CONNECTIONS,
    IN_MEMORY,
    STANDALONE_ASYNC_CONNECTION,
    MockTokenBucketConfig,
    async_run,
    async_tokenbucket_factory,
    initialize_async_connection,
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
    """Test that n requests complete in expected time based on refill frequency."""
    connection = await initialize_async_connection(connection_factory())
    config = MockTokenBucketConfig(refill_frequency=frequency)
    bucket = async_tokenbucket_factory(connection=connection, config=config)
    # Ensure n tasks never complete in less than n/(refill_frequency * refill_amount)
    tasks = [
        asyncio.create_task(
            async_run(
                bucket,
                sleep_duration=0,
            )
        )
        for _ in range(n + 1)  # one added to account for initial capacity of 1
    ]

    start = time.perf_counter()
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start
    assert abs(timeout - elapsed) <= 0.01  # Flaky test, potentially set this to 0.1 later


@pytest.mark.parametrize("connection_factory", [STANDALONE_ASYNC_CONNECTION, IN_MEMORY])
async def test_sleep_is_non_blocking(connection_factory: partial[Redis]) -> None:
    """Test that token bucket sleep does not block other coroutines."""

    async def _sleep(sleep_duration: float) -> None:
        await asyncio.sleep(sleep_duration)

    # Create a bucket with 2 slots available (initial_tokens will default to capacity=2)
    bucket = async_tokenbucket_factory(
        connection=connection_factory(),
        config=MockTokenBucketConfig(capacity=2, refill_amount=2),
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
        asyncio.create_task(async_run(bucket, sleep_duration=1)),
        asyncio.create_task(_sleep(sleep_duration=1)),
        asyncio.create_task(async_run(bucket, sleep_duration=1)),
        asyncio.create_task(_sleep(sleep_duration=1)),
        asyncio.create_task(async_run(bucket, sleep_duration=1)),
        asyncio.create_task(_sleep(sleep_duration=1)),
        asyncio.create_task(async_run(bucket, sleep_duration=1)),
        asyncio.create_task(_sleep(sleep_duration=1)),
    ]

    # All tasks should complete in ~2 seconds if things are working correctly
    await asyncio.wait_for(timeout=2.1, fut=asyncio.gather(*tasks))


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
async def test_high_concurrency_token_acquisition(
    connection_factory: ConnectionFactory,
) -> None:
    """Test many concurrent tasks accessing the same bucket."""
    connection = await initialize_async_connection(connection_factory())

    bucket = async_tokenbucket_factory(
        connection=connection,
        config=MockTokenBucketConfig(capacity=5, refill_frequency=0.1, refill_amount=5),
    )

    tasks = [asyncio.create_task(async_run(bucket, 0)) for _ in range(100)]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start

    # Should take roughly (100-5)/5 * 0.1 = ~1.9 seconds
    assert 2 >= elapsed >= 1.8


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
def test_repr(connection_factory: ConnectionFactory) -> None:
    """Test the string representation of the AsyncTokenBucket."""
    config = MockTokenBucketConfig(name="test", capacity=1)
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
    connection = await initialize_async_connection(connection_factory())

    config = MockTokenBucketConfig(
        capacity=5.0,
        refill_frequency=refill_frequency,
        refill_amount=refill_amount,
        tokens_to_consume=tokens_to_consume,
    )

    # Spawn buckets on different connections(Redis only) with the same key
    tasks = [
        asyncio.create_task(
            async_run(
                async_tokenbucket_factory(connection=connection, config=config),
                sleep_duration=0,
            )
        )
        for _ in range(expected_requests)
    ]

    start = time.perf_counter()
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start

    # Allow for some timing tolerance
    assert abs(timeout - elapsed) <= 0.1


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
async def test_async_max_sleep(connection_factory: ConnectionFactory) -> None:
    """Test that MaxSleepExceededError is raised when max_sleep is exceeded."""
    # Make two requests that will exceed max_sleep
    config = MockTokenBucketConfig(max_sleep=0.1)
    # Build expected error message with actual config values
    expected_msg = (
        rf"^Rate limit exceeded for '{config.name}': would sleep "
        rf"[0-9]+\.[0-9]{{2}}s but max_sleep is {config.max_sleep}s\. "
        rf"Consider increasing capacity \({config.capacity}\) or "
        rf"refill_rate \({config.refill_amount}/{config.refill_frequency}s\)\.$"
    )

    bucket = async_tokenbucket_factory(connection=connection_factory(), config=config)

    async with bucket:
        pass

    with pytest.raises(MaxSleepExceededError, match=expected_msg):
        async with bucket:
            pass  # pragma: no cover


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
    connection = await initialize_async_connection(connection_factory())

    config = MockTokenBucketConfig(capacity=5, initial_tokens=initial_tokens)
    bucket = async_tokenbucket_factory(connection=connection, config=config)

    # Test the value is set correctly
    assert bucket.initial_tokens == expected_value

    # Test behavior: expected_requests immediate + 1 after refill = ~1 second total
    start = time.perf_counter()

    # Assuming the capacity is 5 and initial_tokens = capacity, we are testing that 5+1 (6) requests will complete within 1 second because it
    # takes one second for an additional token to be issued.

    tasks = [asyncio.create_task(async_run(bucket, 0)) for _ in range(expected_requests + 1)]
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start

    assert 0.9 <= elapsed <= 1.1


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
async def test_dynamic_tokens_to_consume(connection_factory: ConnectionFactory) -> None:
    """Test that tokens_to_consume can be passed dynamically when using the async context manager."""
    config = MockTokenBucketConfig(capacity=10.0, refill_frequency=1.0, refill_amount=2.0, tokens_to_consume=1.0)
    connection = await initialize_async_connection(connection_factory())
    bucket = async_tokenbucket_factory(connection=connection, config=config)

    start = time.perf_counter()

    # Use default (1 token)
    async with bucket:
        pass

    # Consume 3 tokens dynamically
    async with bucket(3):
        pass

    async with bucket(5):
        pass

    async with bucket:
        pass

    elapsed = time.perf_counter() - start

    # Total: 1 + 3 + 5 + 1 = 10 tokens consumed
    # Capacity is 10, so all tokens are available immediately
    assert elapsed < 0.1

    start = time.perf_counter()

    # This should require waiting for refills since we just used all 10 tokens
    # We need 5 more tokens, and refill is 2 tokens/second
    # So we need to wait ~2 seconds (2 tokens at 1s, 2 more at 2s)
    async with bucket(3):
        pass

    elapsed = time.perf_counter() - start

    # Should be around 2 seconds (need 3 tokens, have 0, refill 2/sec: 0->2->4 tokens)
    assert 1.9 <= elapsed <= 2.1


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
async def test_zero_cost_operations(connection_factory: ConnectionFactory) -> None:
    """Test that 0 cost operations don't consume tokens or cause delays."""
    config = MockTokenBucketConfig(capacity=2.0, refill_frequency=1.0, refill_amount=1.0, tokens_to_consume=1.0)
    connection = await initialize_async_connection(connection_factory())
    bucket = async_tokenbucket_factory(connection=connection, config=config)

    start = time.perf_counter()

    # Use 2 tokens from capacity
    async with bucket(1):
        pass
    async with bucket(1):
        pass

    # Perform 10 more zero-cost operations - should complete instantly
    for _ in range(10):
        async with bucket(0):
            pass

    # This should require waiting for 1 token refill (~1 second)
    async with bucket(1):
        pass

    elapsed = time.perf_counter() - start
    # Should be around 1 second
    assert 0.9 <= elapsed <= 1.1
