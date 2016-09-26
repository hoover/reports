from django.core.management.base import BaseCommand
from ... import indexing

class Command(BaseCommand):

    help = "Push metrics data to elasticsearch"

    def add_arguments(self, parser):
        parser.add_argument('-a', action='store_true', dest='all')

    def handle(self, all, **options):
        indexing.push_metrics(all)
