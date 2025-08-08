from datetime import datetime
from logging import getLogger
from types import TracebackType
from typing import Annotated, ClassVar

from pydantic import BaseModel, Field
from redis.asyncio.client import Pipeline
from redis.asyncio.cluster import ClusterPipeline

from redis_limiters import MaxSleepExceededError
from redis_limiters.base import AsyncLuaScriptBase, SyncLuaScriptBase

logger = getLogger(__name__)

PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]


class SemaphoreBase(BaseModel):
    name: str
    capacity: PositiveInt = 5
    expiry: PositiveInt = 30
    max_sleep: NonNegativeFloat = 30

    @property
    def key(self) -> str:
        """Key to use for the Semaphore list."""
        return f"{{limiter}}:semaphore:{self.name}"

    @property
    def exists(self) -> str:
        """Key to use when checking if the Semaphore list has been created or not."""
        return f"{{limiter}}:semaphore:{self.name}-exists"

    def __str__(self) -> str:
        return f"Semaphore instance for queue {self.key}"


class SyncSemaphore(SemaphoreBase, SyncLuaScriptBase):
    script_name: ClassVar[str] = "semaphore.lua"

    def __enter__(self) -> None:
        """Call the semaphore Lua script to create a semaphore, then call BLPOP to acquire it."""
        # Retrieve timestamp for when to wake up from Redis
        # To understand what exists does, check the Lua script
        if self.script(
            keys=[self.key, self.exists],
            args=[self.capacity],
        ):
            logger.info("Created new semaphore `%s` with capacity %s", self.name, self.capacity)
        else:
            logger.debug("Skipped creating semaphore, since one exists")

        start = datetime.now()

        self.connection.blpop([self.key], self.max_sleep)
        pipeline = self.connection.pipeline()
        pipeline.expire(self.key, self.expiry)
        pipeline.expire(self.exists, self.expiry)
        pipeline.execute()

        # Raise an exception if we exceeded `max_sleep`
        if 0.0 < self.max_sleep < (datetime.now() - start).total_seconds():
            raise MaxSleepExceededError("Max sleep exceeded waiting for Semaphore")

        logger.debug("Acquired semaphore %s", self.name)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pipeline = self.connection.pipeline()
        pipeline.lpush(self.key, 1)
        pipeline.expire(self.key, self.expiry)
        pipeline.expire(self.exists, self.expiry)
        pipeline.execute()

        logger.debug("Released semaphore %s", self.name)


class AsyncSemaphore(SemaphoreBase, AsyncLuaScriptBase):
    script_name: ClassVar[str] = "semaphore.lua"

    async def __aenter__(self) -> None:
        """Call the semaphore Lua script to create a semaphore, then call BLPOP to acquire it."""
        # Retrieve timestamp for when to wake up from Redis

        if await self.script(
            keys=[self.key, self.exists],
            args=[self.capacity],
        ):
            logger.info("Created new semaphore `%s` with capacity %s", self.name, self.capacity)
        else:
            logger.debug("Skipped creating semaphore, since one exists")

        start = datetime.now()

        await self.connection.blpop([self.key], self.max_sleep)  # type: ignore[union-attr]
        pipeline: Pipeline | ClusterPipeline = self.connection.pipeline()
        pipeline.expire(self.key, self.expiry)  # type: ignore[union-attr]
        pipeline.expire(self.exists, self.expiry)  # type: ignore[union-attr]
        await pipeline.execute()

        # Raise an exception if we waited too long
        if 0.0 < self.max_sleep < (datetime.now() - start).total_seconds():
            raise MaxSleepExceededError(f"Max sleep ({self.max_sleep}s) exceeded waiting for Semaphore")

        logger.debug("Acquired semaphore %s", self.name)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pipeline: Pipeline[str] | ClusterPipeline[str] = self.connection.pipeline()
        pipeline.lpush(self.key, 1)  # type: ignore[union-attr]
        pipeline.expire(self.key, self.expiry)  # type: ignore[union-attr]
        pipeline.expire(self.exists, self.expiry)  # type: ignore[union-attr]
        await pipeline.execute()

        logger.debug("Released semaphore %s", self.name)
