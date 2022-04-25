import logging
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(filename='es.log', level='INFO')
log = logging.getLogger()
log.setLevel(level='INFO')


dsl = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.environ.get('POSTGRES_HOST'),
    'port': os.environ.get('POSTGRES_PORT'),
    'options':"-c search_path=content",
}

es_conf = [{
    'host': os.getenv('ELASTIC_HOST'),
    'port': os.getenv('ELASTIC_PORT'),
}]
