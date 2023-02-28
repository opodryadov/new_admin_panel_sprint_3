import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from backoff import backoff
from models import FilmworkSchema


class ElasticsearchLoader(Elasticsearch):
    """
    Этот класс забирает данные в подготовленном формате и загружает их в Elasticsearch
    """

    def __init__(self, host: str) -> None:
        super(ElasticsearchLoader, self).__init__(hosts=host)
        self.create_index()

    @backoff()
    def create_index(self):
        if not self.indices.exists(index='movies'):
            json_data = json.loads(open('es_schema.json').read())
            self.indices.create(index='movies', body=json_data)

    @backoff()
    def load(self, data: list[FilmworkSchema]) -> None:
        items = [{
            '_index': 'movies',
            '_id': item.id,
            '_source': item.json()
        } for item in data]
        bulk(self, items)
