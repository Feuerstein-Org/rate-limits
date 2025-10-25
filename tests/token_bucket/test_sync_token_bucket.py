import re
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial

import pytest
from redis import Redis
from redis.cluster import RedisCluster

from redis_limiters import MaxSleepExceededError
from tests.conftest import (
    IN_MEMORY,
    STANDALONE_SYNC_CONNECTION,
    SYNC_CONNECTIONS,
    MockTokenBucketConfig,
    sync_run,
    sync_tokenbucket_factory,
)

ConnectionFactory = partial[Redis] | partial[RedisCluster] | Callable[[], None]


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
    config = MockTokenBucketConfig(refill_frequency=frequency)
    bucket = sync_tokenbucket_factory(connection=connection, config=config)

    # Ensure n tasks never complete in less than n/(refill_frequency * refill_amount)
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=n + 1) as executor:
        futures = [
            executor.submit(sync_run, bucket, 0)
            for _ in range(n + 1)  # one added to account for initial capacity of 1
        ]
        # Wait for all threads to complete
        for future in futures:
            future.result()

    elapsed = time.perf_counter() - start
    assert abs(timeout - elapsed) <= 0.01  # Flaky test, potentially set this to 0.1 later


@pytest.mark.parametrize("connection_factory", [STANDALONE_SYNC_CONNECTION, IN_MEMORY])
def test_sleep_is_non_blocking(connection_factory: partial[Redis]) -> None:
    def _sleep(sleep_duration: float) -> None:
        time.sleep(sleep_duration)

    # Create a bucket with 2 slots available (initial_tokens will default to capacity=2)
    bucket = sync_tokenbucket_factory(
        connection=connection_factory(),
        config=MockTokenBucketConfig(capacity=2, refill_amount=2),
    )

    start = time.perf_counter()

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
    # The interleaved _sleep tasks verify that token acquisition doesn't block other threads
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures: list[Future[None]] = [
            executor.submit(sync_run, bucket, 1),
            executor.submit(_sleep, 1),
            executor.submit(sync_run, bucket, 1),
            executor.submit(_sleep, 1),
            executor.submit(sync_run, bucket, 1),
            executor.submit(_sleep, 1),
            executor.submit(sync_run, bucket, 1),
            executor.submit(_sleep, 1),
        ]

        # All tasks should complete in ~2 seconds if things are working correctly
        for future in futures:
            future.result(timeout=2.1)
        elapsed = time.perf_counter() - start

        # Verify timing is within expected range
        assert elapsed <= 2.1


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_high_concurrency_token_acquisition(
    connection_factory: ConnectionFactory,
) -> None:
    """Test many concurrent threads accessing the same bucket"""
    bucket = sync_tokenbucket_factory(
        connection=connection_factory(),
        config=MockTokenBucketConfig(capacity=5, refill_frequency=0.1, refill_amount=5),
    )

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(sync_run, bucket, 0) for _ in range(100)]
        # Wait for all threads to complete
        for future in futures:
            future.result()

    elapsed = time.perf_counter() - start

    # Should take roughly (100-5)/5 * 0.1 = ~1.9 seconds
    assert 2 >= elapsed >= 1.8


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_repr(connection_factory: ConnectionFactory) -> None:
    config = MockTokenBucketConfig(name="test", capacity=1)
    tb = sync_tokenbucket_factory(connection=connection_factory(), config=config)
    assert re.match(r"Token bucket instance for queue {limiter}:token-bucket:test", str(tb))


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
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
def test_token_bucket_tokens_to_consume(  # noqa: PLR0913
    connection_factory: ConnectionFactory,
    tokens_to_consume: float,
    refill_frequency: float,
    refill_amount: float,
    expected_requests: int,
    timeout: float,
) -> None:
    """Test that tokens_to_consume parameter correctly controls token consumption per request."""
    connection = connection_factory()
    config = MockTokenBucketConfig(
        capacity=5.0,
        refill_frequency=refill_frequency,
        refill_amount=refill_amount,
        tokens_to_consume=tokens_to_consume,
    )

    start = time.perf_counter()

    # Spawn buckets on different connections(Redis only) with the same key
    with ThreadPoolExecutor(max_workers=expected_requests) as executor:
        futures = [
            executor.submit(sync_run, sync_tokenbucket_factory(connection=connection, config=config), sleep_duration=0)
            for _ in range(expected_requests)
        ]
        # Wait for all threads to complete
        for future in futures:
            future.result()

    elapsed = time.perf_counter() - start

    # Allow for some timing tolerance
    assert abs(timeout - elapsed) <= 0.1


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_sync_max_sleep(connection_factory: ConnectionFactory) -> None:
    # Make two requests that will exceed max_sleep
    config = MockTokenBucketConfig(max_sleep=0.1)
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


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
@pytest.mark.parametrize(
    "initial_tokens,expected_value,expected_requests",
    [
        (None, 5, 5),  # defaults to capacity
        (2.0, 2.0, 2),  # explicit value
    ],
)
def test_initial_tokens(
    connection_factory: ConnectionFactory, initial_tokens: float | None, expected_value: float, expected_requests: int
) -> None:
    """Test that initial_tokens defaults to capacity or uses explicit value and affects behavior."""
    config = MockTokenBucketConfig(capacity=5, initial_tokens=initial_tokens)
    bucket = sync_tokenbucket_factory(connection=connection_factory(), config=config)

    # Test the value is set correctly
    assert bucket.initial_tokens == expected_value

    # Test behavior: expected_requests immediate + 1 after refill = ~1 second total
    start = time.perf_counter()

    # Assuming the capacity is 5 and initial_tokens = capacity, we are testing that 5+1 (6) requests will complete within 1 second because it
    # takes one second for an additional token to be issued.

    with ThreadPoolExecutor(max_workers=expected_requests + 1) as executor:
        futures = [executor.submit(sync_run, bucket, 0) for _ in range(expected_requests + 1)]
        # Wait for all threads to complete
        for future in futures:
            future.result()

    elapsed = time.perf_counter() - start

    assert 0.9 <= elapsed <= 1.1
