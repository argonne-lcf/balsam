from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
   help = 'Remove Apps'

   def add_arguments(self, parser):
      # Positional arguments
      parser.add_argument('--pk', nargs='+', type=int,help='remove the specified apps.',required=True)

   def handle(self, *args, **options):
      if options['pk'] is not None:
         apps = models.ApplicationDefinition.objects.filter(pk__in=options['pk'])
      
         for app in apps:
            logger.info('About to remove App pk = ' + str(app.pk) + ' \n' + str(app))
            answer = raw_input(' Enter "yes" to continue: ')
            if answer == 'yes':
               app.delete()
               logger.info('App deleted')
            else:
               logger.info('App not deleted')
         
