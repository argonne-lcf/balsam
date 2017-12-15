from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
import logging
logging.basicConfig(
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                   )
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Delete jobs'

    def handle(self, *args, **options):
        
         jobs = models.BalsamJob.objects.all()
         for job in jobs:
             logger.info(' removing job: ' + str(job.id) )
             job.delete()
         
