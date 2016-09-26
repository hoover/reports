from pathlib import Path
import json
from django.conf import settings
from elasticsearch import Elasticsearch

metrics = Path(settings.METRICS_PATH)
es = Elasticsearch(settings.ELASTICSEARCH_URL)

def fixup(data):
    if 'username' in data:
        data['user'] = data.pop('username')

    if data['time'] < 1467632853000: # 2016-07-04, 14:47:33
        if data['type'] in ['search', 'document']:
            data['collections'] = ['maldini']

def get_latest_doc():
    res = es.search(
        index=settings.ELASTICSEARCH_INDEX,
        body={'sort': [{'time': 'desc'}], 'size': 1},
    )
    for hit in res['hits']['hits']:
        return hit['_id']
    return ''

def push_source(source_dir, all):
    source = source_dir.name
    files = sorted(source_dir.iterdir())
    latest_doc = '' if all else get_latest_doc()
    count = 0
    if not all:
        files = files[-2:]
    for file in files:
        with file.open() as lines:
            for n, line in enumerate(lines, 1):
                doc_id = 'users.{}.{:06d}'.format(file.stem, n)
                if doc_id <= latest_doc:
                    continue
                data = json.loads(line)
                data['time'] = int(data['time'] * 1000)
                fixup(data)
                es.index(
                    index=settings.ELASTICSEARCH_INDEX,
                    doc_type=source,
                    id=doc_id,
                    body=data,
                )
                count += 1
            print(file.stem, n)
    print(count)

def push_metrics(all):
    push_source(metrics / 'users', all)
