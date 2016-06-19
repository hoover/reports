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
        day = '2016-06-12'
        with (metrics / 'users' / '{}.txt'.format(day)).open() as lines:
            for n, line in enumerate(lines, 1):
                doc_id = 'users.{}.{:06d}'.format(day, n)
                data = json.loads(line)
                es.index(
                    index=settings.ELASTICSEARCH_INDEX,
                    doc_type='event',
                    id=doc_id,
                    body=data,
                )
