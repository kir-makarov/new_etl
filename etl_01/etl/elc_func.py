import json
import logging
from datetime import datetime

from elasticsearch import Elasticsearch, helpers

from srv import backoff
from state_func import JsonFileStorage, State

log = logging.getLogger('ESLog')


class ElasticLoader:
    def __init__(self, host: list, index_name, state_key='key'):
        self.client = Elasticsearch(host)
        self.index_name = index_name
        self.data = []
        self.key = state_key

    @backoff()
    def create_index(self, index_schema) -> None:
        index = self.index_name
        exist = self.client.indices.exists(index=index)
        if not exist:
            self.client.indices.create(index=index, body=index_schema)
            log.warning(f'{datetime.now()}\n\nСоздание индекса')
        log.warning(f'{datetime.now()}\n\nИндекс уже существует')

    @backoff()
    def bulk_data(self) -> None:
        self.client.bulk(index=self.index_name, body=self.data, refresh=True)

    def load_data_es(self, query: list, state_file: str) -> None:
        data_json = json.dumps(query)
        load_json = json.loads(data_json)
        for row in load_json:
            for i in row:
                if row[i] is None:
                    row[i] = []
            self.data.append({"index": {"_index": self.index_name, "_id": row["id"]}})
            self.data.append(row)
            self.bulk_data()
            self.data.clear()
        State(JsonFileStorage(file_path=state_file)).set_state(str(self.key), value=str(datetime.now().astimezone()))
