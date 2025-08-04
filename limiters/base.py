from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict
from redis import Redis as SyncRedis
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.cluster import RedisCluster as SyncRedisCluster
from redis.commands.core import AsyncScript, Script


class SyncLuaScriptBase(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    connection: SyncRedis | SyncRedisCluster
    script_name: ClassVar[str]
    script: Script = None  # type: ignore[assignment]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        # https://github.com/redis/redis-py/issues/3712
        # Load script on initialization
        with open(Path(__file__).parent / self.script_name) as f:
            self.script = self.connection.register_script(f.read())  # type: ignore


class AsyncLuaScriptBase(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    connection: AsyncRedis | AsyncRedisCluster
    script_name: ClassVar[str]
    script: AsyncScript = None  # type: ignore[assignment]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        # https://github.com/redis/redis-py/issues/3712
        # Load script on initialization
        with open(Path(__file__).parent / self.script_name) as f:
            self.script = self.connection.register_script(f.read())  # type: ignore
