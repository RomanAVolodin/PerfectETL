import datetime
import time
from contextlib import closing
from typing import Type

from psycopg2.extras import DictCursor

from etl.extractors.base_filmwork_extractor import BaseFilmworkExtractor
from etl.loders.filmwork_loader import FilmworkLoader
from etl.transformers.filmwork_transformer import FilmworkTransformer
from helpers.state import State, RedisStorage
from storage_clients.elasticsearch_client import ElasticsearchClient
from storage_clients.postgres_client import PostgresClient
from storage_clients.redis_client import RedisClient


def movie_etl(settings, extractor_type: Type[BaseFilmworkExtractor], state_key: str, timeout: int = 2.5):
    """Factory of etl pipes"""

    with closing(PostgresClient(settings.pg_dsn, cursor_factory=DictCursor)) as pg_conn, \
            closing(ElasticsearchClient(settings.elk_dsn)) as elk_conn, \
            closing(RedisClient(settings.redis_dsn)) as redis_conn:
        pg_conn: PostgresClient
        elk_conn: ElasticsearchClient
        redis_conn: RedisClient

        state = State(RedisStorage(redis_conn), state_key)

        if not state.exists():
            state.set(str(datetime.datetime.min))

        loader = FilmworkLoader(
            elk_conn=elk_conn,
            state=state,
            elk_index=settings.elk_index,
            load_chunk=settings.load_chunk,
        )
        transformer = FilmworkTransformer(
            load_pipe=loader.load,
        )
        extractor = extractor_type(
            pg_conn=pg_conn,
            state=state,
            extract_chunk=settings.extract_chunk,
            transform_pipe=transformer.transform,
        )
        while True:
            extractor.extract()
            time.sleep(timeout)
