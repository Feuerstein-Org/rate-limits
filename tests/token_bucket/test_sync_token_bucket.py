"""Test synchronous token bucket implementation."""

import re
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from functools import partial

import pytest
from pytest_mock import MockerFixture
from redis import Redis
from redis.cluster import RedisCluster

from steindamm import MaxSleepExceededError, NoTokensAvailableError
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
    """Test that n requests complete in expected time based on refill frequency."""
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
    """Test that token bucket sleep does not block other threads."""

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
    """Test many concurrent threads accessing the same bucket."""
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
    """Test the string representation of the SyncTokenBucket."""
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

    bucket = sync_tokenbucket_factory(connection=connection_factory(), config=config)

    with bucket:
        pass

    with (
        pytest.raises(MaxSleepExceededError, match=expected_msg),
        bucket,
    ):
        pass


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


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_initial_tokens_zero(connection_factory: ConnectionFactory) -> None:
    """Test that initial_tokens=0 instantly raises MaxSleepExceededError when max_sleep is exceeded."""
    # Create a fresh config with unique name to avoid Redis key collisions
    config = MockTokenBucketConfig(capacity=5.0, initial_tokens=0.0, max_sleep=0.1)
    bucket = sync_tokenbucket_factory(connection=connection_factory(), config=config)

    with pytest.raises(MaxSleepExceededError), bucket:
        pass


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_dynamic_tokens_to_consume(connection_factory: ConnectionFactory) -> None:
    """Test that tokens_to_consume can be passed dynamically when using the context manager."""
    config = MockTokenBucketConfig(capacity=10.0, refill_frequency=1.0, refill_amount=2.0, tokens_to_consume=1.0)
    bucket = sync_tokenbucket_factory(connection=connection_factory(), config=config)

    start = time.perf_counter()

    # Use default (1 token)
    with bucket:
        pass

    # Consume 3 tokens dynamically
    with bucket(3):
        pass

    with bucket(5):
        pass

    with bucket:
        pass

    elapsed = time.perf_counter() - start

    # Total: 1 + 3 + 5 + 1 = 10 tokens consumed
    # Capacity is 10, so all tokens are available immediately
    assert elapsed < 0.1

    start = time.perf_counter()

    # This should require waiting for refills since we just used all 10 tokens
    # We need 5 more tokens, and refill is 2 tokens/second
    # So we need to wait ~2 seconds (2 tokens at 1s, 2 more at 2s)
    with bucket(3):
        pass

    elapsed = time.perf_counter() - start

    # Should be around 2 seconds (need 3 tokens, have 0, refill 2/sec: 0->2->4 tokens)
    assert 1.9 <= elapsed <= 2.1


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_zero_cost_operations(connection_factory: ConnectionFactory) -> None:
    """Test that 0 cost operations don't consume tokens or cause delays."""
    config = MockTokenBucketConfig(capacity=2.0, refill_frequency=1.0, refill_amount=1.0, tokens_to_consume=1.0)
    bucket = sync_tokenbucket_factory(connection=connection_factory(), config=config)

    start = time.perf_counter()

    # Use 2 tokens from capacity
    with bucket(1):
        pass
    with bucket(1):
        pass

    # Perform 10 more zero-cost operations - should complete instantly
    for _ in range(10):
        with bucket(0):
            pass

    # This should require waiting for 1 token refill (~1 second)
    with bucket(1):
        pass

    elapsed = time.perf_counter() - start
    # Should be around 1 second
    assert 0.9 <= elapsed <= 1.1


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_no_sleep_when_tokens_available(connection_factory: ConnectionFactory, mocker: "MockerFixture") -> None:
    """Test that time.sleep is not called when tokens are immediately available."""
    mock_sleep = mocker.patch("time.sleep")

    config = MockTokenBucketConfig(capacity=5.0)
    bucket = sync_tokenbucket_factory(connection=connection_factory(), config=config)

    # First 5 operations should have tokens available (capacity=5)
    # and should NOT call time.sleep
    for _ in range(5):
        with bucket(1):
            pass

    # Verify time.sleep was never called
    mock_sleep.assert_not_called()

    # The 6th operation requires waiting for a refill and SHOULD call time.sleep
    with bucket(1):
        pass

    # Now time.sleep should have been called exactly once
    mock_sleep.assert_called_once()


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
@pytest.mark.parametrize(
    "window_start_time_offset, tokens_to_consume_first, timeout_first, tokens_to_consume_second, timeout_second",
    [
        # Window started 3s ago, we're 3s into a 5s window with 2s remaining until next refill.
        # Consume all 5 tokens immediately (no wait), then wait 2s for next window to get 5 more tokens.
        (3.0, 5, 0, 5, 2.0),
        (4.0, 3, 0, 3, 1.0),
        (2.0, 4, 0, 5, 3.0),
    ],
)
async def test_window_start_time_alignment(  # noqa: PLR0913
    connection_factory: ConnectionFactory,
    window_start_time_offset: float,
    tokens_to_consume_first: int,
    timeout_first: float,
    tokens_to_consume_second: int,
    timeout_second: float,
) -> None:
    """Test that window_start_time aligns bucket windows to specific timestamps."""
    # Set window_start_time to x seconds ago with 5-second windows
    window_start = datetime.fromtimestamp(time.time() - window_start_time_offset)

    config = MockTokenBucketConfig(
        capacity=5,
        refill_frequency=5,  # 5 second windows
        refill_amount=5,
        window_start_time=window_start,
    )
    bucket = sync_tokenbucket_factory(connection=connection_factory(), config=config)

    # First request
    start = time.perf_counter()
    with bucket(tokens_to_consume_first):
        pass
    elapsed_first = time.perf_counter() - start
    assert timeout_first - 0.1 <= elapsed_first <= timeout_first + 0.1

    # Second request
    start = time.perf_counter()
    with bucket(tokens_to_consume_second):
        pass
    elapsed_second = time.perf_counter() - start
    assert timeout_second - 0.1 <= elapsed_second <= timeout_second + 0.1


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_non_refilling_bucket_consumption_and_exhaustion(connection_factory: ConnectionFactory) -> None:
    """Test that a non-refilling bucket consumption and exhaustion."""
    connection = connection_factory()

    config = MockTokenBucketConfig(
        capacity=10.0,
        refill_frequency=0.0,
        refill_amount=0.0,
        initial_tokens=10.0,
    )
    bucket = sync_tokenbucket_factory(connection=connection, config=config)

    start = time.perf_counter()

    # Should be able to consume all 10 tokens immediately
    with bucket(3):
        pass
    with bucket(3):
        pass
    with bucket(4):
        pass

    elapsed = time.perf_counter() - start
    # All operations should complete immediately since we have enough tokens
    assert elapsed < 0.1

    # Attempting to consume more should raise NoTokensAvailableError
    expected_msg = (
        rf"Token bucket '{config.name}' has run out of tokens\. "
        rf"Available: 0(\.0)?, Requested: 1(\.0)?\. "
        rf"This is a non-refilling bucket \(refill_amount=0(\.0)?, refill_frequency=0(\.0)?\)\."
    )

    with (
        pytest.raises(NoTokensAvailableError, match=expected_msg),
        bucket(1),
    ):
        pass


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_non_refilling_bucket_with_low_initial_tokens(connection_factory: ConnectionFactory) -> None:
    """Test non-refilling bucket with initial_tokens less than capacity."""
    connection = connection_factory()

    config = MockTokenBucketConfig(
        capacity=10.0,
        refill_frequency=0.0,
        refill_amount=0.0,
        initial_tokens=3.0,
    )
    bucket = sync_tokenbucket_factory(connection=connection, config=config)

    # Should be able to consume up to 3 tokens
    with bucket(3):
        pass

    # Attempting to consume more should raise NoTokensAvailableError
    with (
        pytest.raises(NoTokensAvailableError),
        bucket(1),
    ):
        pass


@pytest.mark.parametrize("connection_factory", SYNC_CONNECTIONS)
def test_non_refilling_bucket_zero_cost_operations(connection_factory: ConnectionFactory) -> None:
    """Test that zero-cost operations work with non-refilling buckets even when exhausted."""
    connection = connection_factory()

    config = MockTokenBucketConfig(
        capacity=2.0,
        refill_frequency=0.0,
        refill_amount=0.0,
        initial_tokens=2.0,
    )
    bucket = sync_tokenbucket_factory(connection=connection, config=config)

    # Consume all tokens
    with bucket(2):
        pass

    # Zero-cost operations should still work
    for _ in range(5):
        with bucket(0):
            pass

    # But attempting to consume tokens should fail
    with (
        pytest.raises(NoTokensAvailableError),
        bucket(1),
    ):
        pass
