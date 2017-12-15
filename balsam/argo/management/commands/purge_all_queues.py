from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from common.MessageInterface import MessageInterface
import os,sys,time,multiprocessing
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
   help = 'Lists jobs stored in the DB'
   logger.debug('Listing jobs in the ARGO Service DB.')

   def handle(self, *args, **options):
      
      mi = MessageInterface()
      mi.host           = settings.RABBITMQ_SERVER_NAME
      mi.port           = settings.RABBITMQ_SERVER_PORT
      mi.ssl_key        = settings.RABBITMQ_SSL_KEY
      mi.ssl_cert       = settings.RABBITMQ_SSL_CERT
      mi.ssl_ca_certs   = settings.RABBITMQ_SSL_CA_CERTS

      mi.exchange_name  = settings.RABBITMQ_USER_EXCHANGE_NAME

      mi.open_blocking_connection()
      mi.purge_queue(settings.RABBITMQ_USER_JOB_QUEUE_NAME)
      mi.close()

      mi.exchange_name  = settings.RABBITMQ_BALSAM_EXCHANGE_NAME
      
      mi.open_blocking_connection()
      mi.purge_queue(settings.RABBITMQ_BALSAM_JOB_STATUS_QUEUE)
      mi.close() 




