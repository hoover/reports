from pathlib import Path

base_dir = Path(__file__).absolute().parent.parent.parent.parent

SECRET_KEY = 'TODO: generate random string'
DEBUG = True
METRICS_PATH = base_dir.parent / 'metrics'
ELASTICSEARCH_URL = 'http://localhost:9200'
ELASTICSEARCH_INDEX = 'metrics'
