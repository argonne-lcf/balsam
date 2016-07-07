from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
    help = 'Add BalsamJob to DB'

    def add_arguments(self, parser):
      # Positional arguments
      parser.add_argument('-e','--name',dest='name',type=str,help='application name',required=False,default='')
      parser.add_argument('-d','--description',dest='description',type=str,help='application description',required=False,default='')
      parser.add_argument('-g','--origin-id',dest='origin_id',type=int,help='An ID from the orginator of the job',required=False,default=0)

      parser.add_argument('-q','--queue',dest='queue',type=str,help='queue name to submit job',required=False,default=settings.BALSAM_DEFAULT_QUEUE)
      parser.add_argument('-p','--project',dest='project',type=str,help='project to use for submit',required=False,default=settings.BALSAM_DEFAULT_PROJECT)
      parser.add_argument('-t','--wall-minutes',dest='wall_time_minutes',type=int,help='wall time to use for submit in minutes',required=True)
      parser.add_argument('-n','--num-nodes',dest='num_nodes',type=int,help='number of nodes to use',required=True)
      parser.add_argument('-m','--processes-per-node',dest='processes_per_node',type=int,help='number of processes to run on each node',required=True)
      parser.add_argument('-u','--scheduler-opts',dest='scheduler_opts',type=str,help='options to pass to the scheduler',required=False,default='')

      parser.add_argument('-a','--application',dest='application',type=str,help='Name of the application to use, this must correspond to an application in the ApplicationDefinition DB.',required=True)
      parser.add_argument('-c','--config-file',dest='config_file',type=str,help='configuration file used by the application config script to output the command line, therefore allowing users to pass command line args in a safe way.',required=False,default='')

      parser.add_argument('-i','--input-url',dest='input_url',type=str,help='Input URL from which input files are copied.',required=False,default='')
      parser.add_argument('-o','--output-url',dest='output_url',type=str,help='Output URL to which output files are copied.',required=False,default='')

    def handle(self, *args, **options):
         logger.info(' Adding BalsamJob to DB:')
         logger.info('      name                = ' + options['name'])
         logger.info('      description         = ' + options['description'])
         logger.info('      origin id           = ' + str(options['origin_id']))
         logger.info('      queue               = ' + options['queue'])
         logger.info('      project             = ' + options['project'])
         logger.info('      wall minutes        = ' + str(options['wall_time_minutes']))
         logger.info('      number of nodes     = ' + str(options['num_nodes']))
         logger.info('      processes per node  = ' + str(options['processes_per_node']))
         logger.info('      schduler options    = ' + options['scheduler_opts'])
         logger.info('      application         = ' + options['application'])
         logger.info('      config file         = ' + options['config_file'])
         logger.info('      input url           = ' + options['input_url'])
         logger.info('      output url          = ' + options['output_url'])
         answer = str(raw_input(' Enter "yes" to continue:'))
         if answer == 'yes':
            app = models.ApplicationDefinition.objects.get(name=options['application'])
            if app is None:
               logger.error(' Application "' + options['application'] + '" does not exist in database, failed to add job to DB.')
               return
            job                        = models.BalsamJob()
            job.balsam_job_id          = models.BalsamJob.generate_job_id()
            job.working_directory      = models.BalsamJob.create_working_path(job.balsam_job_id)
            job.site                   = settings.BALSAM_SITE
            job.job_name               = options['name']
            job.job_description        = options['description']
            job.origin_id              = options['origin_id']
            job.queue                  = options['queue']
            job.project                = options['project']
            job.wall_time_minutes      = options['wall_time_minutes']
            job.num_nodes              = options['num_nodes']
            job.processes_per_node     = options['processes_per_node']
            job.scheduler_opts         = options['scheduler_opts']
            job.application            = options['application']
            job.config_file            = options['config_file']
            job.input_url              = options['input_url']
            job.output_url             = options['output_url']
            job.save()
            logger.info('BalsamJob Added to DB')
            logger.info('   pk = ' + str(app.pk))
            logger.info('  balsam id = ' + str(job.balsam_job_id))
            logger.info('  working directory = ' + job.working_directory)
         else:
            logger.info('Application not added to DB')
         
