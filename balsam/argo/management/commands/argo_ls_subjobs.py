from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from argo.models import ArgoJob,ArgoSubJob
import os,sys,time,multiprocessing
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
   help = 'Lists subjobs stored in the DB, '
   logger.debug('Listing subjobs in the ARGO Service DB.')

   def add_arguments(self,parser):
      parser.add_argument('-pk', nargs='+', type=int, help="If given, only speficied pks will be reported.")

   def handle(self, *args, **options):
      process = {}
      
      subjobs = []
      if options['pk'] is None:
         subjobs = ArgoSubJob.objects.all()
      else:
         subjobs = ArgoSubJob.objects.filter(pk__in=options['pk'])
      logger.info(str(len(subjobs)) + ' subjobs in the DB')
      if len(subjobs) > 0:
        list = '\n\n'
        list += ArgoSubJob.get_header() + '\n'
        list += '---------------------------------------------------------------------------------------------------------------------------------------------------------------------\n'
        for subjob in subjobs:
           list += subjob.get_line_string() + '\n'
        list += '\n\n'
        logger.info(list)
      




