import subprocess,sys,shlex,os,logging,datetime
logger = logging.getLogger(__name__)

from django.conf import settings
from balsam.schedulers import exceptions,jobstates
from common import run_subprocess

logger.info(' Using the Cobalt Scheduler ')

def submit(job,cmd):
   ''' should submit a job to the queue and raise a pre-defined sheduler exception if something fails'''
   logger.info("Submitting Cobalt job: %d", job.id )
   logger.debug("Submitting command: " + cmd)

   # set options base on cpus_per_node
   # if job.scheduler_config are set, ignore this.
   options = ''
   """
   if job.scheduler_config != '':
      options = job.scheduler_config
   elif job.processes_per_node < 2:
      options = '--mode c1'
   elif job.processes_per_node < 3:
      options = '--mode c2'
   elif job.processes_per_node < 5:
      options = '--mode c4'
   elif job.processes_per_node < 9:
      options = '--mode c8'
   elif job.processes_per_node < 17:
      options = '--mode c16'
   elif job.processes_per_node < 33:
      options = '--mode c32'
   elif job.processes_per_node < 65:
      options = '--mode c64'
   else:
      options = ''
   """
   
   #command = '%s --run_project -A %s -q %s -n %d -t %d --cwd %s %s %s' % (settings.BALSAM_SCHEDULER_SUBMIT_EXE, 
   command = '%s -A %s -q %s -n %d -t %d --cwd %s %s %s' % (settings.BALSAM_SCHEDULER_SUBMIT_EXE, 
             job.project,
             job.queue,
             job.num_nodes,
             job.wall_time_minutes,
             job.working_directory,
             options,
             cmd)
   logger.debug('CobaltScheduler command = %s', command)
   if settings.BALSAM_SUBMIT_JOBS:
      try:
         output = run_subprocess.run_subprocess(command)
         output = output.strip()
         try:
             scheduler_id = int(output)
         except ValueError:
             scheduler_id = int(output.split()[-1])
         logger.debug('CobaltScheduler job (pk=' + str(job.pk) + ') submitted to scheduler as job ' + str(output))
         job.scheduler_id = scheduler_id
      except run_subprocess.SubprocessNonzeroReturnCode as e:
         raise exceptions.SubmitNonZeroReturnCode('CobaltScheduler submit command returned non-zero value. command = "' + command +'", exception: ' + str(e))
      except run_subprocess.SubprocessFailed as e:
         raise exceptions.SubmitSubprocessFailed('CobaltScheduler subprocess to run commit command failed with exception: ' + str(e))
   else:
     raise exceptions.JobSubmissionDisabled('CobaltScheduler Job submission disabled')

   logger.debug('CobaltScheduler Job submission complete')

def status(joblist):
   try:
      command = settings.BALSAM_SCHEDULER_STATUS_EXE
      output = run_subprocess.run_subprocess(command)

      return output

   except:
      logger.exception('Error in update_status')
   logger.info('Leaving update_status')


def get_job_status(job):
   ''' should return a scheduler job state for the job passed '''
   qstat = QStat(job.scheduler_id)

   # if job is done, qstat doesn't return anything
   if qstat.qstat_line is None:
      logger.debug('CobaltScheduler qstat was empty ')
      return jobstates.JOB_FINISHED

   # parse returned state from qstat
   cobalt_state = qstat.qstat_line.state

   if (cobalt_state.find('running') >= 0 
       or cobalt_state.find('starting') >= 0
       or cobalt_state.find('exiting') >= 0
      ):
      return jobstates.JOB_RUNNING
   elif cobalt_state.find('queued') >= 0:
      return jobstates.JOB_QUEUED

   raise exceptions.JobStatusFailed('CobaltScheduler could not parse qstat for job status.')

def get_queue_state(queue_name):
   '''
   Note: queue_name is ignored for now
   '''
   # - this is rudimentary for now: it determines whether a block of suitable size 
   #   is idle at the moment, in which case the estimate returned is zero. Otherwise, it
   #   returns a large estimate
   class QueueState:
      pass

   command = 'partlist'
   p1 = subprocess.Popen( command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
   output,err = p1.communicate()
   outputlines = output.split('\n')[2:-1]
   #print outputlines
   Q = [o.split()[:3] for o in outputlines]
   Q = filter( lambda x: x[2] == 'idle', Q )
   sizemap = {}
   for q in Q:
      #print q
      location = q[0]
      siz = location.split('-')[-1]
      sizemap[siz] = 1
   sizes = [int(s) for s in sizemap.keys()]
   queue_state = QueueState()

   if sizes:
      queue_state.max_size = max(sizes)
   else:
      queue_state.max_size = 0
   return queue_state

def postprocess(job): pass

class QStat:
   def __init__(self,scheduler_id):
      self.qstat_line = None
      qstat_cmd = settings.BALSAM_SCHEDULER_STATUS_EXE + ' ' + str(scheduler_id)

      try:
         p = subprocess.Popen(shlex.split(qstat_cmd),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      except:
         logger.exception(' received exception while trying to run qstat: ' + str(sys.exc_info()[1]))

      stdout,stderr = p.communicate()
      stdout = stdout.decode('utf-8')
      stderr = stderr.decode('utf-8')
      logger.debug(' qstat ouput: \n' + stdout )
      if p.returncode != 0:
         logger.exception(' return code for qstat is non-zero. stdout = \n' + stdout + '\n stderr = \n' + stderr )
      
      if len(stdout) > 0:
         lines = stdout.split('\n')
         if len(lines) >= 2:
            self.qstat_line = QStatLine.instanceFromQstatLine(lines[2])
      

class QStatLine:
   def __init__(self):
      self.job_id    = None
      self.user      = None
      self.walltime  = None
      self.nodes     = None
      self.state     = None
      self.location  = None

   @staticmethod
   def instanceFromQstatLine(line):
      qstat_line = QStatLine()
      list = line.split()
      if len(list) < 6:
         logger.warning(' failed to parse qstat line: ' + line)
         return None
      qstat_line.job_id    = int(list[0])
      qstat_line.user      = str(list[1])
      qstat_line.walltime  = str(list[2])
      qstat_line.nodes     = int(list[3])
      qstat_line.state     = str(list[4])
      qstat_line.location  = str(list[5])
      return qstat_line
         
