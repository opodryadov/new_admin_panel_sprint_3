from backoff import backoff

from models import FilmworkSchema, PersonSchema, GenreSchema


class DataTransform:
    """
    В этом классе данные преобразуются из формата Postgres в формат, пригодный для Elasticsearch
    """

    @staticmethod
    def get_roles(list_of_persons: list[dict], roles: tuple) -> dict:
        persons_by_role = {}
        for role in roles:
            persons = [
                {"id": field["person_id"], "full_name": field["person_name"]}
                for field in list_of_persons
                if field["person_role"] == role
            ]

            names = [name["full_name"] for name in persons]
            persons_by_role[role] = (persons, names)

        return persons_by_role

    def gendata_film_works(
        self, film_works: list[dict]
    ) -> list[FilmworkSchema]:
        es_film_works = []
        roles = ("director", "actor", "writer")

        for film_work in film_works:
            film_persons = self.get_roles(film_work["persons"], roles)

            film = FilmworkSchema(
                id=film_work["id"],
                imdb_rating=film_work["rating"],
                genre=film_work["genres"],
                title=film_work["title"],
                description=film_work["description"],
                directors_names=film_persons["director"][1],
                actors_names=film_persons["actor"][1],
                writers_names=film_persons["writer"][1],
                directors=film_persons["director"][0],
                actors=film_persons["actor"][0],
                writers=film_persons["writer"][0],
            )

            es_film_works.append(film)

        return es_film_works

    def gendata_persons(self, persons: list[list]) -> list[PersonSchema]:
        es_persons = []

        for person_data in persons:
            person = PersonSchema(
                id=person_data[0],
                full_name=person_data[1],
            )

            es_persons.append(person)

        return es_persons

    def gendata_genres(self, genres: list[list]) -> list[GenreSchema]:
        es_genres = []

        for genre_data in genres:
            genre = GenreSchema(
                id=genre_data[0],
                name=genre_data[1],
            )

            es_genres.append(genre)

        return es_genres

    def get_gendata_transform(self, index: str, data: list[list]):
        return {
            "movies": self.gendata_film_works,
            "persons": self.gendata_persons,
            "genres": self.gendata_genres,
        }[index](data)

    @backoff()
    def transform(self, index: str, data: list[list]):
        return self.get_gendata_transform(index, data)
