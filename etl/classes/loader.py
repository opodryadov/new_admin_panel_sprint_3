import json
import logging

from backoff import backoff
from constants import INDEX_SCHEMA
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


logger = logging.getLogger(__name__)


class ElasticsearchLoader(Elasticsearch):
    """
    Этот класс забирает данные в подготовленном формате и загружает их в Elasticsearch
    """

    def __init__(self, host: str) -> None:
        super(ElasticsearchLoader, self).__init__(hosts=host)
        self.create_index()

    @backoff()
    def create_index(self):
        for index, schema_path in INDEX_SCHEMA:
            if not self.indices.exists(index=index):
                json_data = json.loads(open(schema_path).read())
                self.indices.create(index=index, body=json_data)

    @backoff()
    def load(self, index: str, data: list) -> None:
        items = [
            {"_index": index, "_id": item.id, "_source": item.json()}
            for item in data
        ]
        processed, errors = bulk(self, items)
        if errors:
            logger.error("Migration failed: %s", errors)
        logger.info("Migration: processed %s errors %s", processed, errors)
