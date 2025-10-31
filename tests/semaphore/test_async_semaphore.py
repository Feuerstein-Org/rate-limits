"""Tests for asynchronous semaphore implementation."""

import asyncio
import logging
import re
import time
from functools import partial
from typing import Any

import pytest
from pydantic import ValidationError
from redis.asyncio.client import Monitor, Redis
from redis.asyncio.cluster import RedisCluster

from steindamm import MaxSleepExceededError
from tests.conftest import (
    ASYNC_CONNECTIONS,
    STANDALONE_ASYNC_CONNECTION,
    SemaphoreConfig,
    async_run,
    async_semaphore_factory,
    initialize_async_connection,
)

logger = logging.getLogger(__name__)

ConnectionFactory = partial[Redis] | partial[RedisCluster]


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
@pytest.mark.parametrize(
    "n, capacity, sleep, timeout",
    [
        (10, 1, 0.1, 1),
        (10, 2, 0.1, 0.5),
        (10, 10, 0.1, 0.1),
        (5, 1, 0.1, 0.5),
    ],
)
async def test_semaphore_runtimes(
    connection_factory: ConnectionFactory,
    n: int,
    capacity: int,
    sleep: float,
    timeout: float,
) -> None:
    """Make sure that the runtime of multiple Semaphore instances conform to our expectations.

    The runtime should never fall below the expected lower bound. If we run 6 instances for
    a Semaphore with a capacity of 5, where each instance sleeps 1 second, then it should
    always take 1 >= seconds to run those.
    """
    connection = await initialize_async_connection(connection_factory())
    if connection is None:  # TODO: Add support for in-memory semaphore
        pytest.skip("In-memory connection does not support semaphore")

    config = SemaphoreConfig(capacity=capacity)
    tasks = [
        asyncio.create_task(
            async_run(
                async_semaphore_factory(connection=connection, config=config),
                sleep_duration=sleep,
            )
        )
        for _ in range(n)
    ]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    assert timeout <= time.perf_counter() - start


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
async def test_sleep_is_non_blocking(connection_factory: ConnectionFactory) -> None:
    connection = connection_factory()

    if connection is None:  # TODO: Add support for in-memory semaphore
        pytest.skip("In-memory connection does not support semaphore")

    async def _sleep(duration: float) -> None:
        await asyncio.sleep(duration)

    tasks = [
        # Create task to acquire a semaphore - should take less than 1 second
        asyncio.create_task(async_run(async_semaphore_factory(connection=connection), 0)),
        # And create another task to normal asyncio sleep for 1 second
        asyncio.create_task(_sleep(1)),
    ]

    # Both tasks should complete in ~1 second if things are working correctly
    await asyncio.wait_for(timeout=1.05, fut=asyncio.gather(*tasks))


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
def test_repr(connection_factory: ConnectionFactory) -> None:
    connection = connection_factory()
    if connection is None:  # TODO: Add support for in-memory semaphore
        pytest.skip("In-memory connection does not support semaphore")
    semaphore = async_semaphore_factory(connection=connection, config=SemaphoreConfig(name="test"))
    assert re.match(r"Semaphore instance for queue {limiter}:semaphore:test", str(semaphore))


@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
@pytest.mark.parametrize(
    "config_params,error",
    [
        ({"name": "test"}, None),
        ({"name": None}, ValidationError),
        ({"name": 1}, ValidationError),
        ({"name": True}, ValidationError),
        ({"capacity": 2}, None),
        ({"capacity": 2.2}, ValidationError),
        ({"capacity": None}, ValidationError),
        ({"capacity": "test"}, ValidationError),
        ({"expiry": 20}, None),
        ({"expiry": 2.2}, ValidationError),
        ({"expiry": None}, ValidationError),
        ({"expiry": "test"}, ValidationError),
        ({"max_sleep": 20}, None),
        ({"max_sleep": 0}, None),
        ({"max_sleep": "test"}, ValidationError),
        ({"max_sleep": None}, ValidationError),
    ],
)
def test_init_types(
    connection_factory: ConnectionFactory, config_params: dict[str, Any], error: type[ValidationError] | None
) -> None:
    connection = connection_factory()
    if connection is None:  # TODO: Add support for in-memory semaphore
        pytest.skip("In-memory connection does not support semaphore")
    if error:
        with pytest.raises(error):
            async_semaphore_factory(connection=connection, config=SemaphoreConfig(**config_params))
    else:
        async_semaphore_factory(connection=connection, config=SemaphoreConfig(**config_params))


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
@pytest.mark.parametrize("connection_factory", ASYNC_CONNECTIONS)
async def test_max_sleep(connection_factory: ConnectionFactory) -> None:
    connection = connection_factory()
    if connection is None:  # TODO: Add support for in-memory semaphore
        pytest.skip("In-memory connection does not support semaphore")
    config = SemaphoreConfig(max_sleep=1.0)
    with pytest.raises(
        MaxSleepExceededError,
        match=r"Max sleep \(1\.0s\) exceeded waiting for Semaphore",
    ):
        await asyncio.gather(
            *[
                asyncio.create_task(
                    async_run(
                        async_semaphore_factory(connection=connection_factory(), config=config),
                        1,
                    )
                )
                for _ in range(3)
            ]
        )


@pytest.mark.parametrize("connection_factory", [STANDALONE_ASYNC_CONNECTION])
async def test_redis_instructions(connection_factory: partial[Redis]) -> None:
    connection: Redis = connection_factory()
    config = SemaphoreConfig(expiry=1)

    # Run once to warm up - otherwise tests get flaky
    await async_run(async_semaphore_factory(connection=connection, config=config), 0)

    m: Monitor
    async with connection.monitor() as m:
        await m.connect()
        await async_run(async_semaphore_factory(connection=connection, config=config), 0)
        assert m.connection is not None

        # We expect the eval to generate these exact calls
        commands = [
            # CLIENT SETINFO x2
            str(await m.connection.read_response()),
            str(await m.connection.read_response()),
            # EVALSHA
            str(await m.connection.read_response()),
            # SETNX
            str(await m.connection.read_response()),
            # BLPOP
            str(await m.connection.read_response()),
            # MULTI
            str(await m.connection.read_response()),
            # EXPIRE
            str(await m.connection.read_response()),
            # EXPIRE
            str(await m.connection.read_response()),
            # EXEC
            str(await m.connection.read_response()),
            # MULTI
            str(await m.connection.read_response()),
            # LPUSH
            str(await m.connection.read_response()),
            # EXPIRE
            str(await m.connection.read_response()),
            # EXPIRE
            str(await m.connection.read_response()),
            # EXEC
            str(await m.connection.read_response()),
        ]
        # Make sure there are no other commands generated
        with pytest.raises(asyncio.TimeoutError):
            # This will time out if there are no other commands
            await asyncio.wait_for(timeout=1, fut=m.connection.read_response())

        # Make sure each command conforms to our expectations
        assert "CLIENT" in commands[0], f"was {commands[0]}"
        assert "CLIENT" in commands[0], f"was {commands[0]}"
        commands = commands[2:]
        assert "EVALSHA" in commands[0], f"was {commands[0]}"
        assert "SETNX" in commands[1], f"was {commands[1]}"
        assert f"{{limiter}}:semaphore:{config.name}-exists" in commands[1], f"was {commands[1]}"
        assert "BLPOP" in commands[2], f"was {commands[2]}"
        assert "MULTI" in commands[3], f"was {commands[3]}"
        assert "EXPIRE" in commands[4], f"was {commands[4]}"
        assert "EXPIRE" in commands[5], f"was {commands[5]}"
        assert "EXEC" in commands[6], f"was {commands[6]}"
        assert "MULTI" in commands[7], f"was {commands[7]}"
        assert "LPUSH" in commands[8], f"was {commands[8]}"
        assert "EXPIRE" in commands[9], f"was {commands[9]}"
        assert f"{{limiter}}:semaphore:{config.name}" in commands[9], f"was {commands[9]}"
        assert "EXPIRE" in commands[10], f"was {commands[10]}"
        assert f"{{limiter}}:semaphore:{config.name}-exists" in commands[10], f"was {commands[10]}"
        assert "EXEC" in commands[11], f"was {commands[11]}"
