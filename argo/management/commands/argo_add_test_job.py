from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from argo import models,UserJobReceiver
from common import Serializer
import os,sys,time,logging
logger = logging.getLogger('console')

class Command(BaseCommand):
   help = 'Adds a test job to the Argo DB'
   logger.debug('Adds a test job to the Argo DB')

   def add_arguments(self,parser):
      parser.add_argument('-s',type=str,help="The name of the Balsam site to submit a job.",default='argo_cluster_dev')
      parser.add_argument('-n',type=int,help='The number of subjobs to include',default=1)
   def handle(self, *args, **options):

      n_subjobs = options['n']
      site = options['s']
      
      argojob = models.ArgoJob()
      argojob.argo_job_id              = models.ArgoJob.generate_job_id()
      argojob.working_directory        = UserJobReceiver.CreateWorkingPath(argojob.argo_job_id)
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
      for i in range(n_subjobs):
         argosubjob                       = models.ArgoSubJob()
         argosubjob.site                  = site
         argosubjob.balsam_job_id         = models.ArgoJob.generate_job_id()
         argosubjob.name                  = 'ARGO Test subjob %i' % i
         argosubjob.description           = 'A subjob of an ARGO test job'
         argosubjob.origin_id             = argojob.argo_job_id
         #argosubjob.queue                 = usersubjob.queue
         #argosubjob.project               = usersubjob.project
         argosubjob.wall_time_minutes     = 60
         argosubjob.num_nodes             = 1
         argosubjob.processes_per_node    = 2
         #argosubjob.scheduler_config_id   = usersubjob.scheduler_config_id
         argosubjob.application           = 'test'
         argosubjob.config_file           = create_test_input(os.path.join(argojob.working_directory,'test_config_'+str(i)+'.txt'))
         argosubjob.input_url =  (
               settings.GRIDFTP_PROTOCOL + 
               settings.GRIDFTP_SERVER + 
               argojob.working_directory
              )
         argosubjob.output_url = ( 
               settings.GRIDFTP_PROTOCOL + 
               settings.GRIDFTP_SERVER + 
               argojob.working_directory
              )
         argosubjob.save()
         subjobs.append(argosubjob.pk)
      argojob.subjob_pk_list = Serializer.serialize(subjobs)
      argojob.save()
      logger.info(' created ArgoJob with id: ' + str(argojob.argo_job_id) + ' (pk=' + str(argojob.pk) +') with ' + str(len(subjobs)) + ' balsam jobs.')
      

def create_test_input(filename):
   filecontent='NAME=hello_test\nFILENAME=%s' % filename
   with open(filename,'w') as f:
      f.write(filecontent)
      f.close()
   return filename



