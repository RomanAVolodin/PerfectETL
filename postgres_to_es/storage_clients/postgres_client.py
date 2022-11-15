import contextlib
from typing import Any

import psycopg2
from psycopg2.extensions import connection as pg_conn, cursor as pg_cursor
from psycopg2.sql import SQL
from pydantic import PostgresDsn

from storage_clients.base_client import AbstractStorage, AbstractClientInterface
from helpers.backoff import backoff, reconnect as storage_reconnect
from helpers.logger import LoggerFactory

logger = LoggerFactory().get_logger()


class PostgresClient(AbstractStorage):
    """Layer over psycopg2 for backoff implementation and closing connections."""
    base_exceptions = psycopg2.OperationalError
    _connection: pg_conn

    def __init__(self, dsn: PostgresDsn, *args, **kwargs):
        super().__init__(dsn, *args, **kwargs)

    @property
    def is_connected(self) -> bool:
        return self._connection and not self._connection.closed

    @backoff(exceptions=base_exceptions)
    def connect(self) -> None:
        self._connection = psycopg2.connect(dsn=self.dsn, *self.args, **self.kwargs)
        logger.info("Established new connection for: `%r.", self)

    @backoff(exceptions=base_exceptions)
    @contextlib.contextmanager
    def cursor(self) -> "PostgresCursor":
        cursor: PostgresCursor = PostgresCursor(self)

        yield cursor

        cursor.close()

    def reconnect(self) -> None:
        super().reconnect()

    @backoff(exceptions=base_exceptions)
    def close(self) -> None:
        super().close()


class PostgresCursor(AbstractClientInterface):
    base_exceptions = psycopg2.OperationalError
    _cursor: pg_cursor

    def __init__(self, connection: PostgresClient, *args, **kwargs):
        self._connection = connection
        self.connect(*args, **kwargs)

    def __repr__(self):
        return f"Postgres cursor with connection dsn: {self._connection.dsn}"

    @property
    def is_cursor_opened(self) -> bool:
        return self._cursor and not self._cursor.closed

    @property
    def is_connection_opened(self) -> bool:
        return self._connection.is_connected

    @property
    def is_connected(self) -> bool:
        return self.is_connection_opened and self.is_cursor_opened

    @backoff(exceptions=base_exceptions)
    def connect(self, *args, **kwargs) -> None:
        # noinspection PyProtectedMember
        self._cursor: pg_cursor = self._connection._connection.cursor(*args, **kwargs)
        logger.debug("Created new cursor for: `%r.", self)

    def reconnect(self) -> None:
        if not self.is_connection_opened:
            logger.debug("Trying to reconnect to: `%r.", self)
            self._connection.connect()

        if not self.is_cursor_opened:
            logger.debug("Trying to create new cursor for: `%r.", self)
            self.connect()

    @backoff(exceptions=base_exceptions)
    def close(self) -> None:
        if self.is_cursor_opened:
            self._cursor.close()
            logger.debug("Cursor closed for:  `%r.", self)

    @backoff(exceptions=(base_exceptions, psycopg2.DatabaseError))
    @storage_reconnect
    def execute(self, query: str | SQL, *args, **kwargs) -> None:
        self._cursor.execute(query, *args, **kwargs)

    @backoff(exceptions=(base_exceptions, psycopg2.DatabaseError))
    @storage_reconnect
    def fetchmany(self, chunk: int) -> list[Any]:
        return self._cursor.fetchmany(size=chunk)
