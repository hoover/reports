from django.core.management.base import BaseCommand
from ... import indexing

class Command(BaseCommand):

    help = "Push metrics data to elasticsearch"

    def add_arguments(self, parser):
        parser.add_argument('-a', action='store_true', dest='all')
        parser.add_argument('source_list', nargs='*')

    def handle(self, all, source_list, **options):
        indexing.push_metrics(all, source_list)
