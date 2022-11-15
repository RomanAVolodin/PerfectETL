import datetime
from typing import Callable, Generator

from helpers.logger import LoggerFactory
from models.filmwork import Filmwork

logger = LoggerFactory().get_logger()


class FilmworkTransformer:
    def __init__(
        self,
        load_pipe: Callable[[], Generator[None, tuple[datetime.datetime, list[Filmwork]] | None, None]]
    ):
        self.load_pipe = load_pipe

    def transform(self):
        """Method to transform data. Send data to loader. Receive data from merger."""
        pipe = self.load_pipe()
        pipe.send(None)

        try:
            while True:
                last_updated, rows = (yield)
                rows: list[Filmwork]
                for row in rows:
                    row.transform()

                pipe.send((last_updated, rows))
        except GeneratorExit:
            logger.debug(
                "Transform loop finished."
            )
