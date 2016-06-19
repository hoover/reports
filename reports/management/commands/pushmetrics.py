from pathlib import Path
import json
from django.core.management.base import BaseCommand
from django.conf import settings
from elasticsearch import Elasticsearch

metrics = Path(settings.METRICS_PATH)
es = Elasticsearch(settings.ELASTICSEARCH_URL)

class Command(BaseCommand):

    help = "Push metrics data to elasticsearch"

    def handle(self, **options):
        for file in (metrics / 'users').iterdir():
            with file.open() as lines:
                for n, line in enumerate(lines, 1):
                    doc_id = 'users.{}.{:06d}'.format(file.stem, n)
                    data = json.loads(line)
                    data['time'] = int(data['time'] * 1000)
                    es.index(
                        index=settings.ELASTICSEARCH_INDEX,
                        doc_type='event',
                        id=doc_id,
                        body=data,
                    )
                print(file.stem, n)
