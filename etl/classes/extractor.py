import logging
from typing import Generator

import psycopg2
from backoff import backoff
from constants import CHANK
from psycopg2.extensions import connection


logger = logging.getLogger(__name__)


class PostgresExtractor:
    """
    В этом классе данные, полученные из Postgres, преобразуются во внутренний формат
    """

    @backoff()
    def __init__(self, conn: connection) -> None:
        self.connection = conn

    @backoff()
    def extract(self, query: str, clause: tuple) -> Generator:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, clause)
                while data := cursor.fetchmany(CHANK):
                    yield data

        except psycopg2.Error as err:
            logger.error(err)
            return
