import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

from pydantic import BaseSettings, PostgresDsn, RedisDsn, AnyHttpUrl

from etl.etl import movie_etl
from etl.extractors.filmwork_extractor import FilmworkExtractor
from etl.extractors.genre_extractor import GenreExtractor
from etl.extractors.person_extractor import PersonExtractor
from helpers.logger import LoggerFactory
from storage_clients.elasticsearch_client import ElasticsearchClient

logger = LoggerFactory().get_logger()


class Settings(BaseSettings):
    pg_dsn: PostgresDsn
    extract_chunk: int
    redis_dsn: RedisDsn
    elk_dsn: AnyHttpUrl
    elk_index: str
    load_chunk: int

    class Config:
        case_sensitive = False
        env_file = '.env'
        env_file_encoding = 'utf-8'


def main():
    settings = Settings()

    with closing(ElasticsearchClient(settings.elk_dsn)) as elk_conn:
        if not elk_conn.index_exists(settings.elk_index):
            logger.warn("ELK index `%s` is missing", settings.elk_index)
            with open('postgres_to_es/index.json', 'r') as f:
                data = json.load(f)
                elk_conn.index_create(settings.elk_index, body=data)

            logger.warn("ELK index `%s` created", settings.elk_index)

    with ThreadPoolExecutor() as pool:
        pool.submit(movie_etl, settings, GenreExtractor, 'genre_data')
        pool.submit(movie_etl, settings, PersonExtractor, 'person_data')
        pool.submit(movie_etl, settings, FilmworkExtractor, 'film_work_data')
        logger.critical("ETL started")


main()
