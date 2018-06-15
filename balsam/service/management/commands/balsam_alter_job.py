from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from balsam import models

# python 2/3 compatibility
try:
    input = raw_input
except NameError:
    pass
import logging
logger = logging.getLogger('console')

class Command(BaseCommand):
    help = 'Edit BalsamJob in DB.'

    def add_arguments(self, parser):
      parser.add_argument('-k','--pk',dest='pk',type=int,help='Must specify the pk of the job you want to edit',required=True)

      parser.add_argument('-b','--balsam-job-id',dest='balsam_job_id',type=int,help='A unique identifier for this job.')

      parser.add_argument('-e','--name',dest='name',type=str,help='application name')
      parser.add_argument('-d','--description',dest='description',type=str,help='application description')
      parser.add_argument('-g','--origin-id',dest='origin_id',type=int,help='An ID from the orginator of the job')

      parser.add_argument('-q','--queue',dest='queue',type=str,help='queue name to submit job')
      parser.add_argument('-p','--project',dest='project',type=str,help='project to use for submit')
      parser.add_argument('-t','--wall-minutes',dest='wall_time_minutes',type=int,help='wall time to use for submit in minutes')
      parser.add_argument('-n','--num-nodes',dest='num_nodes',type=int,help='number of nodes to use')
      parser.add_argument('-m','--processes-per-node',dest='processes_per_node',type=int,help='number of processes to run on each node')
      parser.add_argument('-u','--scheduler-opts',dest='scheduler_opts',type=str,help='options to pass to the scheduler')
      parser.add_argument('-w','--scheduler-id',dest='scheduler_id',type=str,help='id assigned by the scheduler')

      parser.add_argument('-a','--application',dest='application',type=str,help='Name of the application to use, this must correspond to an application in the ApplicationDefinition DB.')
      parser.add_argument('-c','--config-file',dest='config_file',type=str,help='configuration file used by the application config script to output the command line, therefore allowing users to pass command line args in a safe way.')


      parser.add_argument('-s','--state',dest='state',type=str,help='current job state')

      parser.add_argument('-i','--input-url',dest='input_url',type=str,help='Input URL from which input files are copied.')
      parser.add_argument('-o','--output-url',dest='output_url',type=str,help='Output URL to which output files are copied.')

      

    def handle(self, *args, **options):
         logger.info('Altering Job pk='+str(options['pk']))
         job = models.BalsamJob.objects.get(pk=options['pk'])
         for option,value in options.iteritems():
            if value is not None and option in job.__dict__.keys():
               logger.info('   changing ' + option + ' from ' + job.__dict__[option] + ' to ' + value)
               job.__dict__[option] = value

         answer = str(input(' Enter "yes" to make changes: '))
         if answer == 'yes':
            logger.info('Saving changes')
            job.save()
         else:
            logger.info('Not saving changes')
