from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from argo.UserJobReceiver import UserJobReceiver
from argo.models import ArgoJob,ArgoSubJob
from common import Serializer
import os,sys,time,multiprocessing,shutil
import logging
logging.basicConfig(
                    level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                   )
logger = logging.getLogger(__name__)



def myHandleError(self, record):
    """
    Handle errors which occur during an emit() call.

    This method should be called from handlers when an exception is
    encountered during an emit() call. If raiseExceptions is false,
    exceptions get silently ignored. This is what is mostly wanted
    for a logging system - most users will not care about errors in
    the logging system, they are more interested in application errors.
    You could, however, replace this with a custom handler if you wish.
    The record which was being processed is passed in to this method.
    """
    if raiseExceptions:
        ei = sys.exc_info()
        try:
            traceback.print_exception(ei[0], ei[1], ei[2], None, sys.stderr)
        except IOError:
            pass    # see issue 5971
        finally:
            del ei
        raise


class Command(BaseCommand):
   help = 'Remove a job stored in the DB'
   logger.debug('Remove a job in the ARGO Service DB.')

   def add_arguments(self,parser):
      parser.add_argument('--pk',dest='pk',nargs='+', type=int,help='remove the jobs with the specified pks.',required=True)
      parser.add_argument('--delete-subjobs',dest='delete_subjobs',action='store_true',default=False,help='if flag specified, the subjobs are deleted too.')

   pass
   def handle(self, *args, **options):


      jobs = ArgoJob.objects.filter(pk__in=options['pk'])
      
      for job in jobs:
         logger.info('removing job: ' + str(job.pk) + ' ' + str(job.job_id) )
         logger.info(' job contains subjobs: ' + job.subjob_pk_list)
         job.delete()
      




