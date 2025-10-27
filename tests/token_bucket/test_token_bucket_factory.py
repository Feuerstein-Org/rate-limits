"""Tests for SyncTokenBucket and AsyncTokenBucket factory classes."""

import pytest

from redis_limiters import (
    AsyncLocalTokenBucket,
    AsyncRedisTokenBucket,
    AsyncTokenBucket,
    SyncLocalTokenBucket,
    SyncRedisTokenBucket,
    SyncTokenBucket,
)
from tests.conftest import (
    CLUSTER_ASYNC_CONNECTION,
    CLUSTER_SYNC_CONNECTION,
    STANDALONE_ASYNC_CONNECTION,
    STANDALONE_SYNC_CONNECTION,
)


class TestSyncTokenBucketFactory:
    """Test that SyncTokenBucket factory returns the correct class based on connection parameter."""

    def test_returns_local_bucket_when_no_connection(self) -> None:
        """Test that SyncTokenBucket returns SyncLocalTokenBucket when connection is None."""
        bucket = SyncTokenBucket(name="test", capacity=10)
        assert isinstance(bucket, SyncLocalTokenBucket)
        assert not isinstance(bucket, SyncRedisTokenBucket)

    def test_returns_redis_bucket_with_standalone_connection(self) -> None:
        """Test that SyncTokenBucket returns SyncRedisTokenBucket with Redis connection."""
        connection = STANDALONE_SYNC_CONNECTION()
        bucket = SyncTokenBucket(connection=connection, name="test", capacity=10)
        assert isinstance(bucket, SyncRedisTokenBucket)
        assert not isinstance(bucket, SyncLocalTokenBucket)

    def test_returns_redis_bucket_with_cluster_connection(self) -> None:
        """Test that SyncTokenBucket returns SyncRedisTokenBucket with RedisCluster connection."""
        connection = CLUSTER_SYNC_CONNECTION()
        bucket = SyncTokenBucket(connection=connection, name="test", capacity=10)
        assert isinstance(bucket, SyncRedisTokenBucket)
        assert not isinstance(bucket, SyncLocalTokenBucket)

    def test_passes_args_to_local_bucket(self) -> None:
        """Test that all kwargs are passed correctly to SyncLocalTokenBucket."""
        bucket = SyncTokenBucket(
            name="test_bucket",
            capacity=100,
            refill_frequency=2.0,
            initial_tokens=25,
            max_sleep=10.0,
            refill_amount=50,
            expiry=60,
            tokens_to_consume=5,
        )
        assert isinstance(bucket, SyncLocalTokenBucket)
        assert bucket.name == "test_bucket"
        assert bucket.capacity == 100
        assert bucket.refill_frequency == 2.0
        assert bucket.initial_tokens == 25
        assert bucket.refill_amount == 50
        assert bucket.max_sleep == 10.0
        assert bucket.expiry == 60
        assert bucket.tokens_to_consume == 5

    def test_passes_args_to_redis_bucket(self) -> None:
        """Test that all kwargs are passed correctly to SyncRedisTokenBucket."""
        connection = STANDALONE_SYNC_CONNECTION()
        bucket = SyncTokenBucket(
            connection=connection,
            name="test_bucket",
            capacity=100,
            refill_frequency=2.0,
            initial_tokens=25,
            max_sleep=10.0,
            refill_amount=50,
            expiry=60,
            tokens_to_consume=5,
        )
        assert isinstance(bucket, SyncRedisTokenBucket)
        assert bucket.name == "test_bucket"
        assert bucket.capacity == 100
        assert bucket.refill_frequency == 2.0
        assert bucket.initial_tokens == 25
        assert bucket.refill_amount == 50
        assert bucket.max_sleep == 10.0
        assert bucket.expiry == 60
        assert bucket.tokens_to_consume == 5

    def test_raises_import_error_when_redis_not_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that ImportError is raised when redis package is not available."""
        # Mock redis as unavailable
        import redis_limiters.token_bucket.token_bucket as tb_module

        monkeypatch.setattr(tb_module, "REDIS_AVAILABLE", False)

        connection = STANDALONE_SYNC_CONNECTION()
        with pytest.raises(
            ImportError,
            match=r"Redis support requires the 'redis' package\. Install it with: pip install redis-limiters\[redis\]",
        ):
            SyncTokenBucket(connection=connection, name="test", capacity=10)


class TestAsyncTokenBucketFactory:
    """Test that AsyncTokenBucket factory returns the correct class based on connection parameter."""

    def test_returns_local_bucket_when_no_connection(self) -> None:
        """Test that AsyncTokenBucket returns AsyncLocalTokenBucket when connection is None."""
        bucket = AsyncTokenBucket(name="test", capacity=10)
        assert isinstance(bucket, AsyncLocalTokenBucket)
        assert not isinstance(bucket, AsyncRedisTokenBucket)

    def test_returns_redis_bucket_with_standalone_connection(self) -> None:
        """Test that AsyncTokenBucket returns AsyncRedisTokenBucket with async Redis connection."""
        connection = STANDALONE_ASYNC_CONNECTION()
        bucket = AsyncTokenBucket(connection=connection, name="test", capacity=10)
        assert isinstance(bucket, AsyncRedisTokenBucket)
        assert not isinstance(bucket, AsyncLocalTokenBucket)

    def test_returns_redis_bucket_with_cluster_connection(self) -> None:
        """Test that AsyncTokenBucket returns AsyncRedisTokenBucket with async RedisCluster connection."""
        connection = CLUSTER_ASYNC_CONNECTION()
        bucket = AsyncTokenBucket(connection=connection, name="test", capacity=10)
        assert isinstance(bucket, AsyncRedisTokenBucket)
        assert not isinstance(bucket, AsyncLocalTokenBucket)

    def test_passes_kwargs_to_local_bucket(self) -> None:
        """Test that all kwargs are passed correctly to AsyncLocalTokenBucket."""
        bucket = AsyncTokenBucket(
            name="test_bucket",
            capacity=100,
            refill_frequency=2.0,
            initial_tokens=25,
            max_sleep=10.0,
            refill_amount=50,
            expiry=60,
            tokens_to_consume=5,
        )
        assert isinstance(bucket, AsyncLocalTokenBucket)
        assert bucket.name == "test_bucket"
        assert bucket.capacity == 100
        assert bucket.refill_frequency == 2.0
        assert bucket.initial_tokens == 25
        assert bucket.refill_amount == 50
        assert bucket.max_sleep == 10.0
        assert bucket.expiry == 60
        assert bucket.tokens_to_consume == 5

    def test_passes_kwargs_to_redis_bucket(self) -> None:
        """Test that all kwargs are passed correctly to AsyncRedisTokenBucket."""
        connection = STANDALONE_ASYNC_CONNECTION()
        bucket = AsyncTokenBucket(
            connection=connection,
            name="test_bucket",
            capacity=100,
            refill_frequency=2.0,
            initial_tokens=25,
            max_sleep=10.0,
            refill_amount=50,
            expiry=60,
            tokens_to_consume=5,
        )
        assert isinstance(bucket, AsyncRedisTokenBucket)
        assert bucket.name == "test_bucket"
        assert bucket.capacity == 100
        assert bucket.refill_frequency == 2.0
        assert bucket.initial_tokens == 25
        assert bucket.refill_amount == 50
        assert bucket.max_sleep == 10.0
        assert bucket.expiry == 60
        assert bucket.tokens_to_consume == 5

    def test_raises_import_error_when_redis_not_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that ImportError is raised when redis package is not available."""
        # Mock redis as unavailable
        import redis_limiters.token_bucket.token_bucket as tb_module

        monkeypatch.setattr(tb_module, "REDIS_AVAILABLE", False)

        connection = STANDALONE_ASYNC_CONNECTION()
        with pytest.raises(
            ImportError,
            match=r"Redis support requires the 'redis' package\. Install it with: pip install redis-limiters\[redis\]",
        ):
            AsyncTokenBucket(connection=connection, name="test", capacity=10)
