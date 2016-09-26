from django.core.management.base import BaseCommand
from ... import indexing

class Command(BaseCommand):

    help = "Reset the elasticsearch index"

    def handle(self, **options):
        indexing.reset_index()
