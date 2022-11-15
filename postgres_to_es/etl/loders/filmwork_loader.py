from helpers.logger import LoggerFactory
from helpers.state import State
from models.filmwork import Filmwork
from storage_clients.elasticsearch_client import ElasticsearchClient

logger = LoggerFactory().get_logger()


class FilmworkLoader:
    def __init__(
            self,
            elk_conn: ElasticsearchClient,
            state: State,
            elk_index: str,
            load_chunk: int,
    ):
        self.elk_conn = elk_conn
        self.state = state
        self.elk_index = elk_index
        self.load_chunk = load_chunk

    def load(self):
        """Method to load data to ELK. Send data to loader. Receive data from transformer."""

        saved_state = None

        try:
            while True:
                last_updated, rows = (yield)
                rows: list[Filmwork]

                if not saved_state:
                    saved_state = last_updated
                elif saved_state != last_updated:
                    # save index on each cycle of fetchmany in produce
                    logger.warn(
                        "Produce cycle finished, updating index: `%s` with value: `%s`", self.state.key, saved_state
                    )
                    self.state.set(str(saved_state))
                    saved_state = last_updated

                data = [{
                    '_op_type': 'update',
                    "_id": row.id,
                    "doc": {
                        "id": row.id,
                        "imdb_rating": row.rating,
                        "title": row.title,
                        "description": row.description,
                        "filmwork_type": row.type,
                        "genres_names": row.genres_names,
                        "genres": [dict(genre) for genre in row.genres],
                        "directors_names": row.directors_names,
                        "actors_names": row.actors_names,
                        "writers_names": row.writers_names,
                        "directors": [dict(director) for director in row.directors],
                        "actors": [dict(actor) for actor in row.actors],
                        "writers": [dict(writer) for writer in row.writers],
                    },
                    "doc_as_upsert": True
                } for row in rows]

                self.elk_conn.chunked_bulk(
                    actions=data, chunk_size=self.load_chunk, index=self.elk_index, raise_on_exception=True
                )

        except GeneratorExit:
            logger.debug(
                "Load cycle finished: `%s`", self.state.key
            )
            if saved_state:
                logger.warn(
                    "Updating index: `%s` with value: `%s`", self.state.key, saved_state
                )
                self.state.set(str(saved_state))
