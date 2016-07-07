from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from argo.UserJobReceiver import UserJobReceiver
from argo.models import ArgoJob
import os,sys,time,multiprocessing
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
   help = 'Lists jobs stored in the DB, '
   logger.debug('Listing jobs in the ARGO Service DB.')

   def add_arguments(self,parser):
      parser.add_argument('-pk', nargs='+', type=int, help="If given, only speficied pks will be reported.")

   def handle(self, *args, **options):
      process = {}
      
      jobs = []
      if options['pk'] is None:
         jobs = ArgoJob.objects.all()
      else:
         jobs = ArgoJob.objects.filter(pk__in=options['pk'])
      logger.info(str(len(jobs)) + ' jobs in the DB')
      if len(jobs) > 0:
        list = '\n\n'
        list += ArgoJob.get_header() + '\n'
        list += '---------------------------------------------------------------------------------------------------------------------------------------------------------------------\n'
        for job in jobs:
           list += job.get_line_string() + '\n'
        list += '\n\n'
        logger.info(list)
      




