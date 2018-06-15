from django.core.management.base import BaseCommand, CommandError
from argo import models
try:
    input = raw_input
except NameError:
    pass
import os,logging
logger = logging.getLogger('console')

fields_to_skip = [
   'time_finished',
   'current_subjob_pk_index',
   'time_created',
   'state',
   'subjob_pk_list',
   'working_directory',
   'time_modified',
   'id',
   'argo_job_id',
   ]

class Command(BaseCommand):
   help = 'Dump a python class file which a User can use to submit an ARGO job.'

   def add_arguments(self,parser):
      parser.add_argument('-o','--output-file',dest='output_filename',help='The filename to which to write the output.',default='ArgoUserJob.py')

   def handle(self, *args, **options):
      logger.info('Dump a ArgoUserJob python class.')

      bj = models.ArgoJob()
      output_filename = options['output_filename']
      if os.path.exists(output_filename):
         raise Exception(' File already exists: ' + output_filename)

      with open(output_filename,'w') as outfile:
         outfile.write('''import json

class ArgoUserJob:
   def __init__(self):
''')
         for var,val in bj.__dict__.iteritems():
            if var[0] == '_': continue
            if var in fields_to_skip: continue
            if isinstance(val,str):
               val = "'" + val + "'"
            outfile.write('      self.' + var + ' = ' + str(val) + '\n')
         outfile.write('''      self.subjobs = []

   def serialize(self):
      return json.dumps(self.__dict__)

   def add_subjob(self,subjob):
      self.subjobs.append(subjob.__dict__)

''')

