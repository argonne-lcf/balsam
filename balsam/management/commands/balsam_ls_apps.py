from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
   help = 'List Apps in the database'

   def add_arguments(self, parser):
      # Positional arguments
      parser.add_argument('--pk', nargs='+', type=int,help='only list certain entries')

   def handle(self, *args, **options):
      logger.info('List Apps in the database:')
      apps = []
      if options['pk'] is None:
         apps = models.ApplicationDefinition.objects.all()
      else:
         apps = models.ApplicationDefinition.objects.filter(pk__in=options['pk'])
      
      logger.info(str(len(apps)) + ' apps in the DB')
      if len(apps) > 0:
        list = '\n\n'
        list += models.ApplicationDefinition.get_header() + '\n'
        list += '---------------------------------------------------------------------------------------------------------------------------------------------------------------------\n'
        for app in apps:
           list += app.get_line_string() + '\n'
        list += '\n\n'
        logger.info(list)
      logger.info('done')
