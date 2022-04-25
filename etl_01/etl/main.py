import logging
import time
from contextlib import closing
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from config import dsl, es_conf
from elc_func import ElasticLoader
from pg_func import PGLoader
from srv import backoff
from load_query import filmwork_query, person_query, genre_query
from indices.filmwork import FILMWORK_INDEX
from indices.genre import GENRE_INDEX
from indices.person import PERSON_INDEX




log = logging.getLogger('MainLog')

if __name__ == '__main__':
    filmwork_columns = ['id', 'title', 'description', 'imdb_rating',
                        'genre', 'director', 'actors_names', 'writers_names',
                        'actors', 'writers']
    genre_columns = ["id", "name"]
    person_columns = ["id", "full_name", "roles", "film_ids"]
    batch = 100


    @backoff()
    def load_data(query, state) -> list:
        with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            log.info(f'{datetime.now()}\n\nПодключение с Postgres установлено. Загрузка данных')
            db = PGLoader(pg_conn, state=state)
            block_records = db.pg_loader(query=query)
        return block_records


    def elastic_saver(query, state, index_schema, index_name, columns) -> None:
        log.info(f'{datetime.now()}\n\nПодключение с ES установлено. Загрузка данных')
        elc = ElasticLoader(es_conf, index_name=index_name)
        elc.create_index(index_schema)
        pg_records = load_data(query=query, state=state)
        count_records = len(pg_records)
        index = 0
        block = []
        while count_records != 0:
            if count_records >= batch:
                for row in pg_records[index: index + batch]:
                    block.append(dict(zip(columns, row)))
                    index += 1
                count_records -= batch
                elc.load_data_es(block, state_file=state)
                block.clear()
            else:
                elc.load_data_es([dict(zip(columns, row)) for row in pg_records[index: index + count_records]],
                                 state_file=state)
                count_records -= count_records


    while True:
        elastic_saver(
            columns=filmwork_columns,
            state="filmwork_data.txt",
            query=filmwork_query,
            index_name="movies",
            index_schema=FILMWORK_INDEX,
        )
        elastic_saver(
            columns=person_columns,
            state="person_data.txt",
            query=person_query,
            index_name="person",
            index_schema=PERSON_INDEX,
        )
        elastic_saver(
            columns=genre_columns,
            state="genre_data.txt",
            query=genre_query,
            index_name="genre",
            index_schema=GENRE_INDEX,
        )

        time.sleep(10)
