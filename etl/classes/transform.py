from backoff import backoff
from models import FilmworkSchema


class DataTransform:
    """
    В этом классе данные преобразуются из формата Postgres в формат, пригодный для Elasticsearch
    """

    @staticmethod
    def get_roles(list_of_persons: list[dict], roles: tuple) -> dict:
        persons_by_role = {}
        for role in roles:
            persons = [{
                'id': field['person_id'],
                'name': field['person_name']
            } for field in list_of_persons if field['person_role'] == role]

            names = [name['name'] for name in persons]
            persons_by_role[role] = (persons, names)

        return persons_by_role

    @backoff()
    def transform(self, film_works: list[dict]) -> list[FilmworkSchema]:
        es_film_works = []
        roles = ('director', 'actor', 'writer')

        for film_work in film_works:
            film_persons = self.get_roles(film_work['persons'], roles)

            film = FilmworkSchema(
                id=film_work['id'],
                imdb_rating=film_work['rating'],
                genre=film_work['genres'],
                title=film_work['title'],
                description=film_work['description'],
                directors_names=film_persons['director'][1],
                actors_names=film_persons['actor'][1],
                writers_names=film_persons['writer'][1],
                directors=film_persons['director'][0],
                actors=film_persons['actor'][0],
                writers=film_persons['writer'][0]
            )

            es_film_works.append(film)

        return es_film_works
