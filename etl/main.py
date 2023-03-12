import datetime
import logging
import time

import psycopg2
from classes import DataTransform, ElasticsearchLoader, PostgresExtractor
from psycopg2.extras import DictCursor
from queries import GET_FILM_WORKS_QUERY, GET_PERSONS_QUERY, GET_GENRES_QUERY
from settings import DSL, ES_HOST, FILE_PATH, INTERVAL
from storage import JsonFileStorage, State


logger = logging.getLogger("etl")


if __name__ == "__main__":
    while True:
        with psycopg2.connect(**DSL, cursor_factory=DictCursor) as conn:
            extractor = PostgresExtractor(conn)
            transformer = DataTransform()
            loader = ElasticsearchLoader(host=ES_HOST)
            storage = JsonFileStorage(FILE_PATH)
            state = State(storage)

            last_modified = state.get_state("modified")
            if last_modified is None:
                last_modified = datetime.date(1, 1, 1).isoformat()

            index_query = (
                ("movies", GET_FILM_WORKS_QUERY, (last_modified,) * 3),
                ("persons", GET_PERSONS_QUERY, (last_modified,)),
                ("genres", GET_GENRES_QUERY, (last_modified,)),
            )

            for index, query, clause in index_query:
                chank_data = extractor.extract(query, clause)
                for data in chank_data:
                    data = transformer.transform(index, data)
                    loader.load(index, data)
                    state.set_state(
                        "modified", datetime.datetime.now().isoformat()
                    )
                    logger.info("The new row loaded: %s", data)

        conn.close()
        time.sleep(float(INTERVAL))
