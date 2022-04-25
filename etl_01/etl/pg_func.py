from datetime import datetime
from state_func import JsonFileStorage, State


class PGLoader:
    def __init__(self, pg_conn, state, state_key='key'):
        self.conn = pg_conn
        self.cursor = self.conn.cursor()
        self.key = state_key
        self.state_key = State(JsonFileStorage(file_path=state)).get_state(state_key)
        self.batch = 100
        self.data_container = []
        self.count = 0

    def get_state_key(self):
        if self.state_key is None:
            return datetime.min
        return self.state_key

    def pg_loader(self, query) -> list:
        self.cursor.execute(query % self.get_state_key())
        while True:
            records = self.cursor.fetchmany(self.batch)
            if not records:
                break
            for row in records:
                self.data_container.append(row)
        return self.data_container


