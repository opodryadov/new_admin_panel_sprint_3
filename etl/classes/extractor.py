import logging
from typing import Generator
from datetime import datetime

import psycopg2
from psycopg2.extensions import connection

from query import query
from backoff import backoff

logger = logging.getLogger(__name__)


class PostgresExtractor:
    """
    В этом классе данные, полученные из Postgres, преобразуются во внутренний формат
    """

    @backoff()
    def __init__(self, conn: connection) -> None:
        self.cursor = conn.cursor()

    @backoff()
    def extract(self, last_modified: datetime) -> Generator:
        try:
            self.cursor.execute(query, (last_modified,) * 3)
            while data := self.cursor.fetchmany(200):
                yield data

        except psycopg2.Error as err:
            logger.error(err)
            return
