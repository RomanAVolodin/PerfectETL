from models.mixins import IdMixIn


class Genre(IdMixIn):
    name: str


class Person(IdMixIn):
    name: str


class Filmwork(IdMixIn):
    rating: str | float | None
    title: str
    description: str | None
    type: str
    genres_names: list[str] | None
    genres: list[Genre]
    directors_names: list[str] | None
    directors: list[Person]
    actors_names: list[str] | None
    actors: list[Person]
    writers_names: list[str] | None
    writers: list[Person]

    @staticmethod
    def _get_names(objects: list[Person] | list[Genre]):
        return [x.name for x in objects]

    def transform(self) -> None:
        self.genres_names = self._get_names(self.genres)
        self.directors_names = self._get_names(self.directors)
        self.actors_names = self._get_names(self.actors)
        self.writers_names = self._get_names(self.writers)
        self.rating = float(self.rating) if self.rating else None
