from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from argo.UserJobReceiver import UserJobReceiver
from argo.models import ArgoJob
import os,sys,time,multiprocessing
import logging
logging.basicConfig(
                    level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                   )
logger = logging.getLogger(__name__)

class Command(BaseCommand):
   help = 'Update settings for jobs stored in the DB, '
   logger.debug('Update settings for jobs in the ARGO Service DB.')

   def add_arguments(self,parser):
      parser.add_argument('--pk',type=int, help="The pk of the ArgoJob to update.",required=True)
      parser.add_argument('--attribute',type=str, help="The attribute to update.",required=True)
      parser.add_argument('--value',type=str, help="The value to set the attribute.",required=True)

   def handle(self, *args, **options):
      
      job = ArgoJob.objects.get(pk=options['pk'])
      logger.info('editing ArgoJob pk=' + str(job.pk) + ' argo_job_id=' + str(job.argo_job_id))

      if hasattr(job,options['attribute']):
         attr = getattr(job,options['attribute'])
         value = str(options['value'])
         if isinstance(attr,int):
            value = int(value)
         elif isinstance(attr,float):
            value = float(value)
         elif isinstance(attr,bool):
            value = bool(value)
         elif isinstance(attr,long):
            value = long(value)

         setattr(job,options['attribute'],value)
         logger.info('setting ' + options['attribute'] + ' to ' + str(value))
         job.save()
      




