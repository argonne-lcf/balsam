from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
from builtins import input
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
    help = 'Add ApplicationDescription to DB'

    def add_arguments(self, parser):
      # Positional arguments
      parser.add_argument('-n','--name',dest='name',help='application name',required=True)
      parser.add_argument('-d','--description',dest='description',help='application description',required=True)
      parser.add_argument('-e','--executable',dest='executable',help='application executable with full path',required=True)
      parser.add_argument('-c','--config-script',dest='config_script',help='configuration script takes a single file and parses its content to output the command line, therefore allowing users to pass command line args in a safe way.',required=True)
      parser.add_argument('-r','--preprocess',dest='preprocess',help='preprocessing script with full path that can be used to process data in the job working directory before the job is submitted to the local batch queue.',required=True)
      parser.add_argument('-o','--postprocess',dest='postprocess',help='postprocessing script with full path that can be used to postprocess data in the job working directory after the job is submitted to the local batch queue.',required=True)
      parser.add_argument('-w','--no-checks',dest='no_checks',help='Typically an exception is thrown if one of the paths specified does not exist, but this flag disables that behavior.',action='store_true')

    def handle(self, *args, **options):
         logger.info(' Adding Application to DB:')
         logger.info('      name             = ' + options['name'])
         logger.info('      description      = ' + options['description'])
         logger.info('      executable       = ' + options['executable'])
         if not os.path.exists(options['executable']):
            raise Exception('executable not found: ' + str(options['executable']))

         logger.info('      config-script    = ' + options['config_script'])
         if not os.path.exists(options['config_script']) or options['no_checks']:
            raise Exception('config-script not found: ' + str(options['config_script']))

         logger.info('      preprocess       = ' + options['preprocess'])
         if not os.path.exists(options['preprocess']) or options['no_checks']:
            raise Exception('preprocess not found: ' + str(options['preprocess']))

         logger.info('      postprocess      = ' + options['postprocess'])
         if not os.path.exists(options['postprocess']) or options['no_checks']:
            raise Exception('postprocess not found: ' + str(options['postprocess']))

         answer = str(input(' Enter "yes" to continue:'))
         if answer == 'yes':
            app = models.ApplicationDefinition()
            app.name = options['name']
            app.description = options['description']
            app.executable = options['executable']
            app.config_script = options['config_script']
            app.preprocess = options['preprocess']
            app.postprocess = options['postprocess']
            app.save()
            logger.info('Application Added to DB, pk = ' + str(app.pk))
         else:
            logger.info('Application not added to DB')
         
