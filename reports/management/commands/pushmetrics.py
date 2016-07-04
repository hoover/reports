from pathlib import Path
import json
from django.core.management.base import BaseCommand
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

class Command(BaseCommand):

    help = "Push metrics data to elasticsearch"

    def get_latest_doc(self):
        res = es.search(
            index=settings.ELASTICSEARCH_INDEX,
            body={'sort': [{'time': 'desc'}], 'size': 1},
        )
        for hit in res['hits']['hits']:
            return hit['_id']
        return ''

    def handle(self, **options):
        latest_doc = self.get_latest_doc()
        count = 0
        for file in (metrics / 'users').iterdir():
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
                        doc_type='event',
                        id=doc_id,
                        body=data,
                    )
                    count += 1
                print(file.stem, n)
        print(count)
