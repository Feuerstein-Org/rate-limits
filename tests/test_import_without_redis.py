"""
Test that steindamm can be imported without redis installed.

Users can import and use local implementationswithout having redis-py installed.
This is a regression test for an issue where importing ANY part of steindamm would fail if
redis wasn't installed, even when only using local (non-Redis) implementations.

The tests use a meta_path import hook to block redis imports, simulating an
environment where redis-py is not installed. This ensures the tests would
catch the issue even in development environments where redis is installed.

Key behaviors tested:
- Local token bucket implementations (AsyncLocalTokenBucket, SyncLocalTokenBucket)
  and factory classes (AsyncTokenBucket, SyncTokenBucket) can be imported
  and used without redis-py installed.
- Redis-specific classes (AsyncRedisTokenBucket, SyncRedisTokenBucket) fail
  with a clear ModuleNotFoundError when redis is not available.
- Semaphore classes currently require redis and fail gracefully.
  (This will change once local semaphore implementations are added)
"""

import os
import subprocess
import sys
from pathlib import Path

# TODO: Update this whole file once local semaphore implementations are added


def test_import_local_classes_without_redis() -> None:
    """Test that local implementations can be imported when redis module is not available."""
    # Get the path to the steindamm source
    steindamm_src = Path(__file__).parent.parent / "src"

    # Python code that simulates redis not being installed
    # We use a clean subprocess to ensure redis import is truly unavailable
    test_code = """
import sys

# Block redis imports to simulate it not being installed
class RedisImportBlocker:
    def find_spec(self, fullname, path, target=None):
        if fullname.startswith('redis'):
            raise ModuleNotFoundError(f"No module named '{fullname}'")
        return None

# Insert the blocker at the beginning of meta_path
sys.meta_path.insert(0, RedisImportBlocker())

# Now test imports - these should work without redis
from steindamm import (
    AsyncTokenBucket,
    SyncTokenBucket,
    AsyncLocalTokenBucket,
    SyncLocalTokenBucket,
    MaxSleepExceededError,
)

# Test creating a local token bucket
bucket = AsyncTokenBucket(name="test_bucket", capacity=10)
assert type(bucket).__name__ == "AsyncLocalTokenBucket", f"Expected AsyncLocalTokenBucket, got {type(bucket).__name__}"

# Test creating a sync local token bucket
sync_bucket = SyncTokenBucket(name="test_sync", capacity=5)
assert type(sync_bucket).__name__ == "SyncLocalTokenBucket", f"Expected SyncLocalTokenBucket, got {type(sync_bucket).__name__}"

print("SUCCESS: All imports work without redis!")
"""

    # Run the test code in a subprocess with steindamm in the path
    result = subprocess.run(
        [sys.executable, "-c", test_code],
        env={**os.environ, "PYTHONPATH": str(steindamm_src)},
        capture_output=True,
        text=True,
        check=False,
    )

    # Check that the test passed
    assert result.returncode == 0, f"Import test failed!\nstdout: {result.stdout}\nstderr: {result.stderr}"
    assert "SUCCESS" in result.stdout, f"Expected success message, got: {result.stdout}"


def test_redis_classes_fail_gracefully_without_redis() -> None:
    """Test that importing Redis classes without redis installed raises ModuleNotFoundError."""
    steindamm_src = Path(__file__).parent.parent / "src"

    test_code = """
import sys

# Block redis imports
class RedisImportBlocker:
    def find_spec(self, fullname, path, target=None):
        if fullname.startswith('redis'):
            raise ModuleNotFoundError(f"No module named '{fullname}'")
        return None

sys.meta_path.insert(0, RedisImportBlocker())

# Try to import Redis-specific classes - should fail with ModuleNotFoundError
try:
    from steindamm import AsyncRedisTokenBucket
    print("ERROR: Should have raised ModuleNotFoundError")
    sys.exit(1)
except ModuleNotFoundError as e:
    # This is expected - Redis classes need redis-py
    if "redis" in str(e).lower():
        print("SUCCESS: Got expected ModuleNotFoundError for AsyncRedisTokenBucket")
    else:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)

try:
    from steindamm import SyncRedisTokenBucket
    print("ERROR: Should have raised ModuleNotFoundError")
    sys.exit(1)
except ModuleNotFoundError as e:
    if "redis" in str(e).lower():
        print("SUCCESS: Got expected ModuleNotFoundError for SyncRedisTokenBucket")
    else:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)
"""

    result = subprocess.run(
        [sys.executable, "-c", test_code],
        env={**os.environ, "PYTHONPATH": str(steindamm_src)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Factory test failed!\nstdout: {result.stdout}\nstderr: {result.stderr}"
    assert result.stdout.count("SUCCESS") == 2, f"Expected 2 success messages, got: {result.stdout}"


def test_factory_classes_work_without_redis() -> None:
    """Test that factory classes (AsyncTokenBucket, SyncTokenBucket) work without redis."""
    steindamm_src = Path(__file__).parent.parent / "src"

    test_code = """
import sys

# Block redis imports
class RedisImportBlocker:
    def find_spec(self, fullname, path, target=None):
        if fullname.startswith('redis'):
            raise ModuleNotFoundError(f"No module named '{fullname}'")
        return None

sys.meta_path.insert(0, RedisImportBlocker())

from steindamm import AsyncTokenBucket, SyncTokenBucket

# Create instances - should use local implementations
async_bucket = AsyncTokenBucket(name="async_test", capacity=10)
sync_bucket = SyncTokenBucket(name="sync_test", capacity=5)

# Verify they're local implementations
assert type(async_bucket).__name__ == "AsyncLocalTokenBucket"
assert type(sync_bucket).__name__ == "SyncLocalTokenBucket"

print("SUCCESS: Factory classes work without redis!")
"""

    result = subprocess.run(
        [sys.executable, "-c", test_code],
        env={**os.environ, "PYTHONPATH": str(steindamm_src)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Import test failed!\nstdout: {result.stdout}\nstderr: {result.stderr}"
    assert "SUCCESS" in result.stdout


def test_semaphores_require_redis() -> None:
    """
    Test that semaphore classes fail gracefully when redis is not available.

    Note: Semaphores currently only have Redis implementations, so they
    require redis-py to be installed. Once local semaphore implementations
    are added, this test should be updated.
    """
    steindamm_src = Path(__file__).parent.parent / "src"

    test_code = """
import sys

# Block redis imports
class RedisImportBlocker:
    def find_spec(self, fullname, path, target=None):
        if fullname.startswith('redis'):
            raise ModuleNotFoundError(f"No module named '{fullname}'")
        return None

sys.meta_path.insert(0, RedisImportBlocker())

# Try to import semaphores - should fail since they need redis
try:
    from steindamm import AsyncSemaphore
    print("ERROR: Should have raised ModuleNotFoundError")
    sys.exit(1)
except ModuleNotFoundError as e:
    if "redis" in str(e).lower():
        print("SUCCESS: Got expected ModuleNotFoundError for AsyncSemaphore")
    else:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)

try:
    from steindamm import SyncSemaphore
    print("ERROR: Should have raised ModuleNotFoundError")
    sys.exit(1)
except ModuleNotFoundError as e:
    if "redis" in str(e).lower():
        print("SUCCESS: Got expected ModuleNotFoundError for SyncSemaphore")
    else:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)
"""

    result = subprocess.run(
        [sys.executable, "-c", test_code],
        env={**os.environ, "PYTHONPATH": str(steindamm_src)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Semaphore test failed!\nstdout: {result.stdout}\nstderr: {result.stderr}"
    assert result.stdout.count("SUCCESS") == 2, f"Expected 2 success messages, got: {result.stdout}"
