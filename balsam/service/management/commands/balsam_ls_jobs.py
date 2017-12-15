from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
    help = 'List BalsamJobs in DB'

    def add_arguments(self, parser):
      parser.add_argument('--pk', nargs='+', type=int,help='only list certain entries')

    def handle(self, *args, **options):
      logger.info('BalsamJobs in the database:')
      jobs = []
      if options['pk'] is None:
         jobs = models.BalsamJob.objects.all()
      else:
         jobs = models.BalsamJob.objects.filter(pk__in=options['pk'])
      
      logger.info(str(len(jobs)) + ' jobs in the DB')
      if len(jobs) > 0:
        list = '\n\n'
        list += models.BalsamJob.get_header() + '\n'
        list += '---------------------------------------------------------------------------------------------------------------------------------------------------------------------\n'
        for job in jobs:
           list += job.get_line_string() + '\n'
        list += '\n\n'
        logger.info(list)
