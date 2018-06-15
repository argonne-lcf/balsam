
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

class Command(BaseCommand):
    help = 'Alter ApplicationDescription to DB'

    def add_arguments(self, parser):
      # Positional arguments
      parser.add_argument('-p','--pk',dest='pk',type=int,help='The DB pk of the application to update.',required=True)
      parser.add_argument('-n','--name',dest='name',help='application name')
      parser.add_argument('-d','--description',dest='description',help='application description')
      parser.add_argument('-e','--executable',dest='executable',help='application executable with full path')
      parser.add_argument('-c','--config-script',dest='config_script',help='configuration script takes a single file and parses its content to output the command line, therefore allowing users to pass command line args in a safe way.')
      parser.add_argument('-r','--preprocess',dest='preprocess',help='preprocessing script with full path that can be used to process data in the job working directory before the job is submitted to the local batch queue.')
      parser.add_argument('-o','--postprocess',dest='postprocess',help='postprocessing script with full path that can be used to postprocess data in the job working directory after the job is submitted to the local batch queue.')
      parser.add_argument('-w','--no-checks',dest='no_checks',help='Typically an exception is thrown if one of the paths specified does not exist, but this flag disables that behavior.',action='store_true')

    def handle(self, *args, **options):
         logger.info(' Altering Application in DB with pk: ' + str(options['pk']))
         app = models.ApplicationDefinition.objects.get(pk=options['pk'])
         if options['name'] is not None:
            app.name = options['name']
            logger.info('      name             = ' + options['name'])
         if options['description'] is not None:
            app.description = options['description']
            logger.info('      description      = ' + options['description'])
         if options['executable'] is not None:
            app.executable = options['executable']
            logger.info('      executable       = ' + options['executable'])
            if not os.path.exists(options['executable']):
               raise Exception('executable not found: ' + str(options['executable']))

         if options['config_script'] is not None:
            app.config_script = options['config_script']
            logger.info('      config-script    = ' + options['config_script'])
            if not os.path.exists(options['config_script']) or options['no_checks']:
               raise Exception('config-script not found: ' + str(options['config_script']))

         if options['preprocess'] is not None:
            app.preprocess = options['preprocess']
            logger.info('      preprocess       = ' + options['preprocess'])
            if not os.path.exists(options['preprocess']) or options['no_checks']:
               raise Exception('preprocess not found: ' + str(options['preprocess']))

         if options['postprocess'] is not None:
            app.postprocess = options['postprocess']
            logger.info('      postprocess      = ' + options['postprocess'])
            if not os.path.exists(options['postprocess']) or options['no_checks']:
               raise Exception('postprocess not found: ' + str(options['postprocess']))

         answer = str(input(' Enter "yes" to continue:'))
         if answer == 'yes':
            app.save()
            logger.info('Application altered in DB, pk = ' + str(app.pk))
         else:
            logger.info('Application not altered in DB')
         
