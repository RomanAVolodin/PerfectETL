import redis.exceptions
from pydantic import RedisDsn
from redis.client import Redis
from redis.typing import KeyT, EncodableT

from storage_clients.base_client import AbstractStorage
from helpers.backoff import backoff, reconnect as storage_reconnect
from helpers.exceptions import RedisNotConnectedError
from helpers.logger import LoggerFactory

logger = LoggerFactory().get_logger()


class RedisClient(AbstractStorage):
    """Layer over Redis for backoff implementation and closing connections."""
    base_exceptions = redis.exceptions.RedisError
    _connection: Redis

    def __init__(self, dsn: RedisDsn, *args, **kwargs):
        super().__init__(dsn, *args, **kwargs)

    @property
    def is_connected(self) -> bool:
        result = True
        try:
            # ping is unsafe
            self._connection and self._connection.ping()
        except redis.exceptions.ConnectionError:
            result = False

        return result

    @backoff(exceptions=(base_exceptions, RedisNotConnectedError))
    def connect(self) -> None:
        self._connection = Redis(
            host=self.dsn.host,
            port=int(self.dsn.port),
            db=self.dsn.path[1:],
            username=self.dsn.user,
            password=self.dsn.password,
            *self.args,
            **self.kwargs,
        )

        if not self.is_connected:
            # client is lazy, need to check it
            raise RedisNotConnectedError(f"Connection is not properly established for: `{self.__repr__()}`")

        logger.info("Established new connection for: `%r.", self)

    def reconnect(self) -> None:
        super().reconnect()

    @backoff(exceptions=base_exceptions)
    def close(self) -> None:
        super().close()

    @backoff(exceptions=base_exceptions)
    @storage_reconnect
    def keys(self, pattern, **kwargs) -> list[str]:
        return self._connection.keys(pattern, **kwargs)

    @backoff(exceptions=base_exceptions)
    @storage_reconnect
    def exists(self, *names: KeyT) -> int:
        return self._connection.exists(*names)

    @backoff(exceptions=base_exceptions)
    @storage_reconnect
    def get(self, name: KeyT) -> bytes | None:
        return self._connection.get(name)

    @backoff(exceptions=base_exceptions)
    @storage_reconnect
    def set(self, name: KeyT, value: EncodableT, *args, **kwargs) -> None:
        return self._connection.set(name, value, *args, **kwargs)
