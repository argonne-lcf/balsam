from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
   help = 'Update job'

   def add_arguments(self, parser):
      # Positional arguments
      parser.add_argument('-p','--pk',dest='pk',type=int,help='The job to update',required=True)
      parser.add_argument( '-a','--attribute',dest='attribute',type=str,help='the attribute to update',required=True)
      parser.add_argument( '-b','--value',dest='value',type=str,help='Update the attribute to this value.',required=True)


   def handle(self, *args, **options):
      job = models.BalsamJob.objects.get(pk=int(options['pk']))
      logger.debug(' updating job ' + str(job))
      if hasattr(job,options['attribute']):
         logger.info(' setting ' + str(options['attribute']) + ' to ' + str(options['value']) + ' for pk = ' + str(options['pk']))
         setattr(job,options['attribute'],options['value'])
         job.save()
        

