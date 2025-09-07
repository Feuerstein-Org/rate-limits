# Python Redis Limiters

A library which regulates traffic, with respect to concurrency or time.
It implements sync and async context managers for a [semaphore](#semaphore)- and a [token bucket](#token-bucket)-implementation.

The rate limiters are distributed, using Redis, and leverages Lua scripts to
improve performance and simplify the code. Lua scripts
run on Redis, and make each implementation fully atomic, while
also reducing the number of round-trips required.

Use is supported for standalone redis instances, and clusters.
We currently only support Python 3.11, but can add support for older versions if needed.

## NOTE:
This project was initially forked from [redis-rate-limiters](https://github.com/otovo/redis-rate-limiters) and was mainly created by Sondre Lillebø Gundersen [link](https://github.com/sondrelg).

The old project is no longer being worked on and only supported PydanticV1. I plan to add more functionality as well as maintain this fork in the future. It will be published under py-redis-limiters.

Currently I:
 - migrated to PydanticV2
 - migrated from poetry to uv
 - migrated from just to mise en place
 - changed the pre-commit & build process a bit (e.g. remove black/isort in favor of ruff)
 - tidied up a few types as well as add types to tests
 - added a few more tests (I plan to add more)
 - added default values to the rate limits.

 Note: The README is currently outdated - I will update it later, for now check the releases page.

## Installation

```
pip install py-redis-limiters
```

## Usage

### Semaphore

The semaphore classes are useful when you have concurrency restrictions;
e.g., say you're allowed 5 active requests at the time for a given API token.

Beware that the client will block until the Semaphore is acquired,
or the `max_sleep` limit is exceeded. If the `max_sleep` limit is exceeded, a `MaxSleepExceededError` is raised. Setting `max_sleep` to 0.0 will sleep "endlessly" - this is also the default value.

Here's how you might use the async version:

```python
import asyncio

from httpx import AsyncClient
from redis.asyncio import Redis

from limiters import AsyncSemaphore

# Every property besides name has a default like below
limiter = AsyncSemaphore(
    name="foo",    # name of the resource you are limiting traffic for
    capacity=5,    # allow 5 concurrent requests
    max_sleep=30,  # raise an error if it takes longer than 30 seconds to acquire the semaphore
    expiry=30,      # set expiry on the semaphore keys in Redis to prevent deadlocks
    connection=Redis.from_url("redis://localhost:6379"),
)

async def get_foo():
    async with AsyncClient() as client:
        async with limiter:
            client.get(...)


async def main():
    await asyncio.gather(
        get_foo() for i in range(100)
    )
```

and here is how you might use the sync version:

```python
import requests
from redis import Redis

from limiters import SyncSemaphore


limiter = SyncSemaphore(
    name="foo",
    capacity=5,
    max_sleep=30,
    expiry=30,
    connection=Redis.from_url("redis://localhost:6379"),
)

def main():
    with limiter:
        requests.get(...)
```

### Token bucket

The `TocketBucket` classes are useful if you're working with time-based
rate limits. Say, you are allowed 100 requests per minute, for a given API token.

If the `max_sleep` limit is exceeded, a `MaxSleepExceededError` is raised. Setting `max_sleep` to 0.0 will sleep "endlessly" - this is also the default value.

Here's how you might use the async version:

```python
import asyncio

from httpx import AsyncClient
from redis.asyncio import Redis

from limiters import AsyncTokenBucket

# Every property besides name has a default like below
limiter = AsyncTokenBucket(
    name="foo",          # name of the resource you are limiting traffic for
    capacity=5,          # hold up to 5 tokens
    refill_frequency=1,  # add tokens every second
    refill_amount=1,     # add 1 token when refilling
    max_sleep=0,         # raise an error if there are no free tokens for X seconds, 0 never expires
    connection=Redis.from_url("redis://localhost:6379"),
)

async def get_foo():
    async with AsyncClient() as client:
        async with limiter:
            client.get(...)

async def main():
    await asyncio.gather(
        get_foo() for i in range(100)
    )
```

and here is how you might use the sync version:

```python
import requests
from redis import Redis

from limiters import SyncTokenBucket


limiter = SyncTokenBucket(
    name="foo",
    capacity=5,
    refill_frequency=1,
    refill_amount=1,
    max_sleep=0,
    connection=Redis.from_url("redis://localhost:6379"),
)

def main():
    with limiter:
        requests.get(...)
```

### Using them as a decorator

We don't ship decorators in the package, but if you would
like to limit the rate at which a whole function is run,
you can create your own, like this:

```python
from limiters import AsyncSemaphore


# Define a decorator function
def limit(name, capacity):
  def middle(f):
    async def inner(*args, **kwargs):
      async with AsyncSemaphore(name=name, capacity=capacity):
        return await f(*args, **kwargs)
    return inner
  return middle


# Then pass the relevant limiter arguments like this
@limit(name="foo", capacity=5)
def fetch_foo(id: UUID) -> Foo:
```

## Contributing

Contributions are very welcome. Here's how to get started:

- Clone the repo
- Install [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Run `pre-commit install` to set up pre-commit
- Install [just](https://just.systems/man/en/) and run `just setup`
  If you prefer not to install just, just take a look at the justfile and
  run the commands yourself.
- Make your code changes, with tests
- Commit your changes and open a PR

## Publishing a new version

To publish a new version:

- Update the package version in the `pyproject.toml`
- Open [Github releases](https://github.com/Feuerstein-Org/py-redis-limiters/releases)
- Press "Draft a new release"
- Set a tag matching the new version (for example, `v0.1.0`)
- Set the title matching the tag
- Add some release notes, explaining what has changed
- Publish

Once the release is published, our [publish workflow](https://github.com/Feuerstein-Org/py-redis-limiters/blob/main/.github/workflows/publish.yaml) should be triggered
to push the new version to PyPI.
