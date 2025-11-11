# Steindamm

A library which regulates traffic, with respect to concurrency or time.
It implements sync and async context managers for a [semaphore](#semaphore)- and a [token bucket](#token-bucket)-implementation.

The rate limiters can be distributed using Redis, or run locally in-memory for single-process applications.

**Redis-based limiters** leverage Lua scripts on Redis, each operation is fully atomic.
Both standalone Redis instances and clusters are supported with sync and async interfaces.

**Local limiters** provide thread-safe (for sync) and asyncio-safe (for async) in-memory rate limiting without requiring Redis.

We currently support Python 3.11, 3.12, and 3.13.

## Features

### Deployment Options
- **Local (In-Memory)**: Thread-safe and asyncio-safe rate limiting without Redis dependencies
  - Perfect for single-process applications, testing, and development
  - Zero external dependencies required
- **Redis-Based**: Distributed rate limiting using Redis with Lua scripts
  - Atomic operations for accuracy in distributed systems
  - Supports both standalone Redis and Redis Cluster
  - Scales across multiple processes and servers

### Async & Sync Support
- Full support for both synchronous and asynchronous code
- Context manager interface (`with` / `async with`)

### Flexibility & Control
- **Factory Classes**: `SyncTokenBucket` and `AsyncTokenBucket` automatically choose implementation based on connection
- **Explicit Classes**: Direct access to `SyncRedisTokenBucket`, `AsyncRedisTokenBucket`, `SyncLocalTokenBucket`, `AsyncLocalTokenBucket`
- **Configurable Token Consumption**: `tokens_to_consume` parameter for variable-cost operations
  - Set at initialization or override dynamically per request: `with bucket(5):`
- **Customizable Behavior**: Control capacity, refill rates, expiry, max sleep time, and initial state

## Installation

### Basic Installation (Local limiters only)
```bash
pip install steindamm
```

### With Redis Support
```bash
pip install steindamm[redis]
```
Or install Redis separately:
```bash
pip install steindamm redis
```

## Usage

### Token bucket

The `TokenBucket` classes are useful if you're working with time-based
rate limits. Say, you are allowed 100 requests per minute, for a given API token.

If the `max_sleep` limit is exceeded, a `MaxSleepExceededError` is raised. Setting `max_sleep` to 0.0 will sleep "endlessly" - this is also the default value. On the other hand `expiry` is how long the token bucket will persist in Redis without any activity (acquires or releases). You might need to adjust both to your requirements.

#### Using Local (In-Memory) Token Bucket

The local token bucket doesn't require Redis and runs entirely in-memory.
Perfect for single-process applications or testing.

> **Note:** The local token bucket implementation does not currently support expiry of bucket state. Buckets persist in memory for the lifetime of the process. If you are creating buckets dynamically (e.g., one bucket per user or per API key), this could lead to unbounded memory growth. Consider using the Redis-based implementation for applications with dynamic bucket creation, or ensure buckets are reused for the same resources.

**Async version:**

```python
import asyncio

from httpx import AsyncClient

from steindamm import AsyncTokenBucket

# No Redis connection needed - runs in-memory
limiter = AsyncTokenBucket(
    name="foo",                # name of the resource you are limiting traffic for
    capacity=5,                # hold up to 5 tokens (default: 5.0)
    refill_frequency=1,        # add tokens every second (default: 1.0)
    refill_amount=1,           # add 1 token when refilling (default: 1.0)
    initial_tokens=None,       # start with full capacity (default: None, which uses capacity)
    max_sleep=30,              # raise an error if there are no free tokens for X seconds, 0 never expires (default: 30.0)
    tokens_to_consume=1,       # consume 1 token per request (default: 1.0)
)

async def get_foo():
    async with AsyncClient() as client:
        async with limiter:
            await client.get(...)

async def main():
    await asyncio.gather(
        get_foo() for i in range(100)
    )
```

**Sync version:**

```python
import requests

from steindamm import SyncTokenBucket

limiter = SyncTokenBucket(
    name="foo",
    capacity=5,
    refill_frequency=1,
    refill_amount=1,
    tokens_to_consume=1,
)

def main():
    with limiter:
        requests.get(...)
```

#### Using Redis-Based Token Bucket

For distributed rate limiting across multiple processes or servers,
use the Redis-based implementation by providing a `connection` parameter.

**Async version:**

```python
import asyncio

from httpx import AsyncClient
from redis.asyncio import Redis

from steindamm import AsyncTokenBucket

# With Redis connection - distributed across processes/servers
limiter = AsyncTokenBucket(
    connection=Redis.from_url("redis://localhost:6379"),  # Add Redis connection for distributed limiting
    name="foo",
    capacity=5,
    refill_frequency=1,
    refill_amount=1,
    max_sleep=30,
    expiry=60,                 # set expiry on Redis keys in seconds (default: 60)
    tokens_to_consume=1,
)

async def get_foo():
    async with AsyncClient() as client:
        async with limiter:
            await client.get(...)

async def main():
    await asyncio.gather(
        get_foo() for i in range(100)
    )
```

**Sync version:**

```python
import requests
from redis import Redis

from steindamm import SyncTokenBucket


limiter = SyncTokenBucket(
    connection=Redis.from_url("redis://localhost:6379"),
    name="foo",
    capacity=5,
    refill_frequency=1,
    refill_amount=1,
    max_sleep=30,
    expiry=60,
    tokens_to_consume=1,
)

def main():
    with limiter:
        requests.get(...)
```

### Semaphore

The semaphore classes are useful when you have concurrency restrictions;
e.g., say you're allowed 5 active requests at the time for a given API token.

**Note:** Currently, only Redis-based semaphores are available. Local (in-memory) semaphore implementation is planned for a future release.

Beware that the client will block until the Semaphore is acquired,
or the `max_sleep` limit is exceeded. If the `max_sleep` limit is exceeded, a `MaxSleepExceededError` is raised. Setting `max_sleep` to 0.0 will sleep "endlessly" - default is 30 seconds. On the other hand `expiry` is how long the semaphore will persist in Redis without any activity (acquires or releases). You might need to adjust both to your requirements.

Here's how you might use the async version:

```python
import asyncio

from httpx import AsyncClient
from redis.asyncio import Redis

from steindamm import AsyncSemaphore

# All properties have defaults except name and connection
limiter = AsyncSemaphore(
    connection=Redis.from_url("redis://localhost:6379"),
    name="foo",    # name of the resource you are limiting traffic for
    capacity=5,    # allow 5 concurrent requests (default: 5)
    max_sleep=30,  # raise an error if it takes longer than 30 seconds to acquire the semaphore (default: 30.0)
    expiry=60,     # set expiry on the semaphore keys in Redis to prevent deadlocks (default: 60)
)

async def get_foo():
    async with AsyncClient() as client:
        async with limiter:
            await client.get(...)


async def main():
    await asyncio.gather(
        get_foo() for i in range(100)
    )
```

and here is how you might use the sync version:

```python
import requests
from redis import Redis

from steindamm import SyncSemaphore


limiter = SyncSemaphore(
    connection=Redis.from_url("redis://localhost:6379"),
    name="foo",
    capacity=5,
    max_sleep=30,
    expiry=60,
)

def main():
    with limiter:
        requests.get(...)
```

#### Using Explicit Implementation Classes

For explicit control over which implementation to use, import the specific classes:

```python
# Local implementations
from steindamm import SyncLocalTokenBucket, AsyncLocalTokenBucket

# Redis implementations
from steindamm import SyncRedisTokenBucket, AsyncRedisTokenBucket

# Use directly without factory logic
local_limiter = SyncLocalTokenBucket(name="api", capacity=10)
redis_limiter = SyncRedisTokenBucket(connection=redis_conn, name="api", capacity=10)
```

#### Consuming Multiple Tokens Per Request

You can control how many tokens are consumed per operation using the `tokens_to_consume` parameter. There are two ways to do this:

**1. Set at initialization time:**

```python
from steindamm import SyncTokenBucket

# Small requests consume 1 token
small_limiter = SyncTokenBucket(name="api", capacity=100, tokens_to_consume=1)

# Large requests consume 5 tokens
large_limiter = SyncTokenBucket(name="api", capacity=100, tokens_to_consume=5)

with small_limiter:
    make_small_request()  # Consumes 1 token

with large_limiter:
    make_large_request()  # Consumes 5 tokens
```
Note: When different operations _on the same resource_ have different "costs", using the same "name" (aka the bucket key) will cause the tokens to be shared across all limiters using that name.


**2. Pass dynamically when using the context manager:**

You can override the `tokens_to_consume` value on a per-request basis by calling the bucket instance:

```python
from steindamm import SyncTokenBucket

# Create a single bucket with default tokens_to_consume=1
limiter = SyncTokenBucket(name="api", capacity=100, tokens_to_consume=1)

with limiter:
    make_small_request()  # Uses default: consumes 1 token

with limiter(5):
    make_large_request()  # Dynamically consumes 5 tokens

with limiter(10):
    make_extra_large_request()  # Dynamically consumes 10 tokens
```

This works for all token bucket implementations (sync/async, Redis/local):

```python
from steindamm import AsyncTokenBucket

async_limiter = AsyncTokenBucket(name="api", capacity=100)

async with async_limiter:
    await make_small_request()  # Default: 1 token

async with async_limiter(5):
    await make_large_request()  # Dynamically: 5 tokens
```



### Using them as a decorator

We don't ship decorators in the package, but if you would
like to limit the rate at which a whole function is run,
you can create your own, like this:

```python
from steindamm import AsyncSemaphore
from redis.asyncio import Redis

# Define a decorator function
def limit(name, capacity, connection):
  def middle(f):
    async def inner(*args, **kwargs):
      async with AsyncSemaphore(connection=connection, name=name, capacity=capacity):
        return await f(*args, **kwargs)
    return inner
  return middle

# Or for local token buckets (no Redis needed)
from steindamm import AsyncTokenBucket

def rate_limit(name, capacity):
  def middle(f):
    async def inner(*args, **kwargs):
      async with AsyncTokenBucket(name=name, capacity=capacity):
        return await f(*args, **kwargs)
    return inner
  return middle


# Then pass the relevant limiter arguments like this
@limit(name="foo", capacity=5, connection=Redis.from_url("redis://localhost:6379"))
async def fetch_foo(id: UUID) -> Foo:
    ...

# Or with local rate limiting
@rate_limit(name="bar", capacity=10)
async def fetch_bar(id: UUID) -> Bar:
    ...
```

## Contributing

Contributions are very welcome. Here's how to get started:

- Clone the repo
- Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and [mise](https://mise.jdx.dev/)
- Run `mise run install` to install dependencies
  If you prefer not to install mise, check the `mise.toml` file and
  run the commands manually.
- Make your code changes, with tests
- Run tests with `mise run test` or `uv run pytest`
  Note that you will need to first spin up the redis docker containers. This can be done with `mise run test-setup` (and shut down with `mise run test-teardown`) or the full cycle can be run with `mise run test-full`.
- Commit your changes and open a PR

## Publishing a new version

To publish a new version:

- Update the package version in the `pyproject.toml`
- Open [Github releases](https://github.com/feuerstein-org/steindamm/releases)
- Press "Draft a new release"
- Set a tag matching the new version (for example, `v0.8.0`)
- Set the title matching the tag
- Add some release notes, explaining what has changed
- Publish

Once the release is published, our [publish workflow](https://github.com/feuerstein-org/steindamm/blob/main/.github/workflows/publish.yaml) should be triggered
to push the new version to PyPI.

## Acknowledgment:
This project was initially forked from [redis-rate-limiters](https://github.com/otovo/redis-rate-limiters) and was mainly created by Sondre Lillebø Gundersen [link](https://github.com/sondrelg). It was no longer maintained and I since rewrote a lot of stuff as well as added a local version of the limiters and new functionality like the initial amount of tokens or how many tokens to consume at once.
