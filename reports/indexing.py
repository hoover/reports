from pathlib import Path
import json
from django.conf import settings
from elasticsearch import Elasticsearch, helpers as es_helpers

metrics = Path(settings.METRICS_PATH)
es = Elasticsearch(settings.ELASTICSEARCH_URL)

def reset_index():
    es.indices.delete(settings.ELASTICSEARCH_INDEX, ignore=[400, 404])
    es.indices.create(settings.ELASTICSEARCH_INDEX, {
      'mappings': {
        'doc': {
          'properties': {
            'time': {'type': 'date'},
            'type': {'type': 'string', 'index': 'not_analyzed'},
            'user': {'type': 'string', 'index': 'not_analyzed'},
          },
        },
      },
    })

def fixup(data, source):
    if source == 'users':
        if 'username' in data:
            data['user'] = data.pop('username')

        if data['time'] < 1467632853: # 2016-07-04, 14:47:33
            if data['type'] in ['search', 'document']:
                data['collections'] = ['maldini']

    if source == 'jobs':
        if 'time' not in data:
            data['time'] = data.pop('start')

    data['source'] = source
    if isinstance(data['time'], float):
        data['time'] = int(data['time'] * 1000)

def get_latest_doc(source):
    res = es.search(
        index=settings.ELASTICSEARCH_INDEX,
        body={
            'query': {'term': {'source': {'value': source}}},
            'sort': [{'time': 'desc'}],
            'size': 1
        },
    )
    for hit in res['hits']['hits']:
        return hit['_id']
    return ''

def push_source(source_dir, all):
    source = source_dir.name
    files = sorted(source_dir.iterdir())
    latest_doc = '' if all else get_latest_doc(source)
    if not all:
        files = files[-2:]

    def iter_lines():
        for file in files:
            with file.open() as lines:
                for n, line in enumerate(lines, 1):
                    doc_id = 'users.{}.{:06d}'.format(file.stem, n)
                    if doc_id <= latest_doc:
                        continue
                    data = json.loads(line)
                    fixup(data, source)
                    data.update({
                        '_op_type': 'index',
                        '_index': settings.ELASTICSEARCH_INDEX,
                        '_type': 'event',
                        '_id': doc_id,
                    })
                    yield data
                print(source, file.stem, n)

    (ok, err) = es_helpers.bulk(es, stats_only=True, actions=iter_lines())
    if err:
        raise RuntimeError("Indexing failures: %d" % err)

    print(ok)

def push_metrics(all, source_list):
    for source_dir in metrics.iterdir():
        if source_list and source_dir.name not in source_list:
            continue
        push_source(source_dir, all)
