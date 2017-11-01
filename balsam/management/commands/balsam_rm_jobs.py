from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
import logging
logger = logging.getLogger('console')
try:
    input = raw_input
except NameError:
    pass

class Command(BaseCommand):
   help = 'Remove BalsamJobs'

   def add_arguments(self, parser):
      # Positional arguments
      parser.add_argument('--pk', nargs='+', type=int,help='remove the specified jobs.',required=True)

   def handle(self, *args, **options):
      if options['pk'] is not None:
         jobs = models.BalsamJob.objects.filter(pk__in=options['pk'])
      
         for job in jobs:
            logger.info('About to delete BalsamJob pk = ' + str(job.pk) + ' \n' + str(job))
            answer = input(' Enter "yes" to continue: ')
            if answer == 'yes':
               job.delete()
               logger.info('BalsamJob deleted')
            else:
               logger.info('BalsamJob not deleted')
         
