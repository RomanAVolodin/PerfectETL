from etl.extractors.base_filmwork_extractor import BaseFilmworkExtractor


class FilmworkExtractor(BaseFilmworkExtractor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.produce_table = 'film_work'

    def extract(self):
        return super().extract()

    def _produce(self):
        return super()._produce()

    @property
    def _enrich_query(self) -> str:
        raise NotImplementedError

    def _enrich(self):
        """No need to enrich for filmwork"""
        pipe = self._merge()
        pipe.send(None)

        try:
            while True:
                last_updated, rows = (yield)
                pipe.send((last_updated, rows))

        except GeneratorExit:
            pass

    def _merge(self):
        return super()._merge()
