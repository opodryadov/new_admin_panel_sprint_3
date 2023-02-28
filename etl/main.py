import os
import time
import logging
import datetime

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

from storage import JsonFileStorage, State
from classes import (
    DataTransform,
    PostgresExtractor,
    ElasticsearchLoader,
)

logger = logging.getLogger('etl')

load_dotenv()

DSL = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASS'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
    'options': '-c search_path=content'
}


if __name__ == '__main__':
    while True:
        with psycopg2.connect(**DSL, cursor_factory=DictCursor) as conn:
            storage = JsonFileStorage(os.environ.get('FILE_PATH'))
            state = State(storage)

            last_modified = state.get_state('modified')
            if last_modified is None:
                last_modified = datetime.date(1, 1, 1)

            extractor = PostgresExtractor(conn)
            film_works = extractor.extract(last_modified)

            transformer = DataTransform()
            loader = ElasticsearchLoader(host=os.environ.get('ES_HOST'))

            for film_work in film_works:
                data = transformer.transform(film_work)
                loader.load(data)
                state.set_state('modified', datetime.datetime.now().isoformat())

                logger.info('The new row loaded: %s', data)

        conn.close()
        time.sleep(float(os.environ.get('INTERVAL')))
