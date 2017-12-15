from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from argo import models,UserJobReceiver
from common import Serializer,transfer
import os,sys,time,logging
logger = logging.getLogger('console')

class Command(BaseCommand):
   help = 'Adds a test job to the Argo DB'
   logger.debug('Adds a test job to the Argo DB')

   def add_arguments(self,parser):
      parser.add_argument('-s','--site',dest='site',type=str,help="The name of the Balsam site to submit a job.",default='argo_cluster_dev')
      parser.add_argument('-j','-nsubjobs',dest='nsubjobs',type=int,help='The number of subjobs to include',default=1)
      parser.add_argument('-e','--serial',dest='serial',action='store_true',help='serialtest job is used instead of mpitestjob.',default=False)
      parser.add_argument('-r','--ranks',dest='ranks',type=int,help='The number of mpi ranks per node to run.',default=1)
      parser.add_argument('-n','--numnodes',dest='numnodes',type=int,help='The number node to run.',default=1)
      parser.add_argument('-l','--local',dest='local',action='store_true',help='files are all local so use "cp" to move',default=False)
   def handle(self, *args, **options):

      nsubjobs    = options['nsubjobs']
      site        = options['site']
      serial      = options['serial']
      ranks       = options['ranks']
      numnodes    = options['numnodes']
      local       = options['local']
      
      argojob = models.ArgoJob()
      argojob.job_id                   = models.ArgoJob.generate_job_id()
      argojob.working_directory        = UserJobReceiver.CreateWorkingPath(argojob.job_id)
      argojob.user_id                  = 0
      argojob.job_name                 = 'test job'
      argojob.job_description          = 'A job to test ARGO/Balsam.'
      argojob.group_identifier         = 'argo_add_test_job'
      argojob.username                 = os.environ['USER']
      #argojob.email                    = userjob.email
      #argojob.input_url                = userjob.input_url
      #argojob.output_url               = userjob.output_url
      #argojob.job_status_routing_key   = userjob.job_status_routing_key

      subjobs = []
      for i in range(nsubjobs):
         argosubjob                       = models.ArgoSubJob()
         argosubjob.site                  = site
         argosubjob.job_id                = models.ArgoJob.generate_job_id()
         argosubjob.name                  = 'ARGO Test subjob %i' % i
         argosubjob.description           = 'A subjob of an ARGO test job'
         argosubjob.job_id                = argojob.job_id
         #argosubjob.queue                 = usersubjob.queue
         #argosubjob.project               = usersubjob.project
         argosubjob.wall_time_minutes     = 60
         argosubjob.num_nodes             = numnodes
         argosubjob.processes_per_node    = ranks
         #argosubjob.scheduler_config_id   = usersubjob.scheduler_config_id
         if serial:
            argosubjob.application        = 'serialtest'
         else:                              # or 
            argosubjob.application        = 'mpitest'
         argosubjob.config_file           = argosubjob.application + '_cmdline.ini'
         # add input/output data locations, either use local or gridftp
         if local:
            argosubjob.input_url          = transfer.LOCAL_PROTOCOL + ':/' + argojob.working_directory
            argosubjob.output_url         = transfer.LOCAL_PROTOCOL + ':/' + argojob.working_directory
         else:
            # add local transfer protocol
            argosubjob.input_url          = (
                                             transfer.GRIDFTP_PROTOCOL + '://' +
                                             settings.GRIDFTP_SERVER + 
                                             argojob.working_directory
                                            )
            argosubjob.output_url         = ( 
                                             transfer.GRIDFTP_PROTOCOL + '://' +
                                             settings.GRIDFTP_SERVER + 
                                             argojob.working_directory
                                            )
         argosubjob.save()
         subjobs.append(argosubjob.pk)
      argojob.subjob_pk_list = Serializer.serialize(subjobs)
      argojob.save()
      logger.info(' created ArgoJob with id: ' + str(argojob.job_id) + ' (pk=' + str(argojob.pk) +') with ' + str(len(subjobs)) + ' balsam jobs.')
      

def create_test_input(filename):
   filecontent='NAME=hello_test\nFILENAME=%s' % filename
   with open(filename,'w') as f:
      f.write(filecontent)
      f.close()
   return filename



