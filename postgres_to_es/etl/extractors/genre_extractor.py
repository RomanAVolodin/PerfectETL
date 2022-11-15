from etl.extractors.base_filmwork_extractor import BaseFilmworkExtractor


class GenreExtractor(BaseFilmworkExtractor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.produce_table = 'genre'

    def extract(self):
        return super().extract()

    def _produce(self):
        return super()._produce()

    @property
    def _enrich_query(self) -> str:
        return """
            SELECT DISTINCT
                fw.id, fw.updated_at
            FROM
                content.film_work fw
            LEFT JOIN
                content.genre_film_work gfw ON gfw.film_work_id = fw.id
            WHERE
                gfw.genre_id IN %s
            ORDER BY
                fw.updated_at;
        """

    def _enrich(self):
        return super()._enrich()

    def _merge(self):
        return super()._merge()
