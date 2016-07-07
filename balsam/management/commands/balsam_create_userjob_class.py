
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
# python 2/3 compatibility
try:
    input = raw_input
except NameError:
    pass
import logging,os
logger = logging.getLogger('console')

fields_to_skip = [
   'time_job_started',
   'time_created',
   'working_directory',
   'id',
   'time_start_queued',
   'state',
   'time_job_finished',
   'scheduler_id',
   'time_modified',
   'balsam_job_id',
   ]

class Command(BaseCommand):
    help = 'Dump a python class which a User can use to submit a Balsam job.'

    def add_arguments(self, parser):
      # Positional arguments
      parser.add_argument('-o','--output-file',dest='output_filename',help='The filename to which to write the output.',default='BalsamUserJob.py')
      

    def handle(self, *args, **options):
      logger.info('Dump a BalsamUserJob python class.')
      
      bj = models.BalsamJob()
      output_filename = options['output_filename']
      if os.path.exists(output_filename):
         raise Exception(' File already exists: ' + output_filename)

      with open(output_filename,'w') as outfile:
         outfile.write('''
class BalsamUserJob:
   def __init__(self):
''')
         for var,val in bj.__dict__.iteritems():
            if var[0] == '_': continue
            if var in fields_to_skip: continue
            if isinstance(val,str):
               val = "'" + val + "'"
            outfile.write('      self.' + var + ' = ' + str(val) + '\n')


         
