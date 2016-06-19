from django.core.management.base import BaseCommand
from django.conf import settings
from elasticsearch import Elasticsearch

es = Elasticsearch(settings.ELASTICSEARCH_URL)

class Command(BaseCommand):

    help = "Reset the elasticsearch index"

    def handle(self, **options):
        es.indices.delete(settings.ELASTICSEARCH_INDEX, ignore=[400, 404])
        es.indices.create(settings.ELASTICSEARCH_INDEX, {
          'mappings': {
            'doc': {
              'properties': {
                'time': {'type': 'date'},
              },
            },
          },
        })
