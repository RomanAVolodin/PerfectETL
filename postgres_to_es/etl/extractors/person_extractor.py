from etl.extractors.base_filmwork_extractor import BaseFilmworkExtractor


class PersonExtractor(BaseFilmworkExtractor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.produce_table = 'person'

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
                content.person_film_work pfw ON pfw.film_work_id = fw.id
            WHERE
                pfw.person_id IN %s
            ORDER BY
                fw.updated_at;
        """

    def _enrich(self):
        return super()._enrich()

    def _merge(self):
        return super()._merge()
