import os,logging,subprocess,sys,datetime,glob,shutil
logger = logging.getLogger('balsam')

from django.conf import settings

from balsam.schedulers import exceptions,jobstates
from balsam.schedulers.condor import condor_q,condor_history,condor_submit

from common.CondorJobDescriptionFile import CondorJobDescriptionFile
from common import run_subprocess


CONDOR_FILE_PATH=os.path.join(settings.INSTALL_PATH,'balsam','schedulers','condor')

job_submit_filename = 'job_submit.conf'
input_tarball_filename = 'condor_job_input.tgz'
untar_input_scriptname = os.path.join(CONDOR_FILE_PATH,'condor_untar_input.sh')
output_tarball_filename = 'condor_job_output.tgz'
tar_output_scriptname = os.path.join(CONDOR_FILE_PATH,'condor_tar_output.sh')

logger.info(' Using the Condor Scheduler ')

# Default Job description parameters for job submit file
class CondorConfig:
   def __init__(self):
      logger.debug(' Building CondorConfig ')
      self.condor_universe        = 'vanilla' # standard, vanilla, parallel, java, 
      self.condor_notifications   = None # Always, Error, Never
      self.condor_executable      = None # comes from user
      self.condor_arguments       = None # comes from user
      self.condor_precmd          = os.path.basename(untar_input_scriptname)
      self.condor_preargs         = input_tarball_filename 
      self.condor_postcmd         = os.path.basename(tar_output_scriptname) # should come from user
      self.condor_postargs        = output_tarball_filename # comes from user
      self.condor_requirements    = None # may not be needed
      self.condor_rank            = None # may not be needed
      self.condor_arch            = None # may not be needed
      self.condor_priority        = 5 # higher the better, but only meaningful within one's own jobs
      self.condor_getenv          = None # copy local environment, don't want this
      self.condor_initialdir      = None 
      self.condor_input           = None
      self.condor_output          = None
      self.condor_error           = None
      self.condor_log             = None
      self.condor_notify_user     = None
      self.condor_should_transfer_files = 'YES'
      self.condor_when_to_transfer_output = 'ON_EXIT_OR_EVICT'
      self.condor_queue           = None
      self.condor_transfer_input_files = [tar_output_scriptname,untar_input_scriptname,input_tarball_filename]
      self.condor_transfer_output_files = [output_tarball_filename]
      self.condor_environment     = None
      self.condor_output          = 'condor_stdout.txt.$(Cluster).$(Process)'
      self.condor_error           = 'condor_stderr.txt.$(Cluster).$(Process)'
      self.condor_log             = 'condor_log.txt.$(Cluster).$(Process)'

      '''
      if job.preprocess:
         self.condor_precmd = os.path.basename(job.preprocess)
         if job.preprocess_arguments:
            self.condor_preargs = str(job.preprocess_arguments)
         # add preprocess to transfer_input_files
         if len(self.condor_transfer_input_files) >= 1:
            self.condor_transfer_input_files += ','
         self.condor_transfer_input_files += job.preprocess
      
      if job.postprocess:
         logger.debug(' postprocess: ' + job.postprocess )
         self.condor_postcmd = os.path.basename(job.postprocess)
         if job.postprocess_arguments:
            self.condor_postargs = str(job.postprocess_arguments)
         # add postprocess to transfer_input_files
         if len(self.condor_transfer_input_files) >= 1:
            self.condor_transfer_input_files += ','
         self.condor_transfer_input_files += job.postprocess
      '''

      #if settings.BALSAM_CONDOR_ENVIRONMENT:
      #   self.condor_environment = settings.BALSAM_CONDOR_ENVIRONMENT
   
   def __str__(self):
      s = ''
      s += '%-032s%s\n' % ('condor_universe',self.condor_universe) 
      s += '%-032s%s\n' % ('condor_notifications',self.condor_notifications)
      s += '%-032s%s\n' % ('condor_executable',self.condor_executable)
      s += '%-032s%s\n' % ('condor_arguments',self.condor_arguments)
      s += '%-032s%s\n' % ('condor_precmd',self.condor_precmd)
      s += '%-032s%s\n' % ('condor_preargs',self.condor_preargs)
      s += '%-032s%s\n' % ('condor_postcmd',self.condor_postcmd)
      s += '%-032s%s\n' % ('condor_postargs',self.condor_postargs)
      s += '%-032s%s\n' % ('condor_requirements',self.condor_requirements)
      s += '%-032s%s\n' % ('condor_rank',self.condor_rank)
      s += '%-032s%s\n' % ('condor_arch',self.condor_arch)
      s += '%-032s%s\n' % ('condor_priority',self.condor_priority)
      s += '%-032s%s\n' % ('condor_getenv',self.condor_getenv)
      s += '%-032s%s\n' % ('condor_initialdir',self.condor_initialdir)
      s += '%-032s%s\n' % ('condor_input',self.condor_input)
      s += '%-032s%s\n' % ('condor_output',self.condor_output)
      s += '%-032s%s\n' % ('condor_error',self.condor_error)
      s += '%-032s%s\n' % ('condor_log',self.condor_log)
      s += '%-032s%s\n' % ('condor_notify_user',self.condor_notify_user)
      s += '%-032s%s\n' % ('condor_should_transfer_files',self.condor_should_transfer_files)
      s += '%-032s%s\n' % ('condor_when_to_transfer_output',self.condor_when_to_transfer_output)
      s += '%-032s%s\n' % ('condor_queue',self.condor_queue)
      s += '%-032s%s\n' % ('condor_transfer_input_files',self.condor_transfer_input_files)
      s += '%-032s%s\n' % ('condor_transfer_output_files',self.condor_transfer_output_files)
      s += '%-032s%s\n' % ('condor_environment',self.condor_environment)
      return s

   def write_file(self,filename):
      ''' uses the job information to create a condor submit file '''
      logger.debug('in write_file')

      txt = ''
            
      if self.condor_universe:                 txt += 'Universe                 = '  + self.condor_universe + '\n'
      if self.condor_notifications:            txt += 'Notifications            = '  + self.condor_notifications + '\n'
      if self.condor_arguments:                txt += 'Arguments                = '  + self.condor_arguments + '\n'
      if self.condor_requirements:             txt += 'Requirements             = '  + self.condor_requirements + '\n'
      if self.condor_rank:                     txt += 'Rank                     = '  + self.condor_rank + '\n'
      if self.condor_priority:                 txt += 'Priority                 = '  + str(self.condor_priority) + '\n'
      if self.condor_getenv:                   txt += 'GetEnv                   = '  + self.condor_getenv + '\n'
      if self.condor_initialdir:               txt += 'Initialdir               = '  + self.condor_initialdir + '\n'
      if self.condor_input:                    txt += 'Input                    = '  + self.condor_input + '\n'
      if self.condor_output:                   txt += 'Output                   = '  + self.condor_output + '\n'
      if self.condor_error:                    txt += 'Error                    = '  + self.condor_error + '\n'
      if self.condor_log:                      txt += 'Log                      = '  + self.condor_log + '\n'
      if self.condor_notify_user:              txt += 'Notify_user              = '  + self.condor_notify_user + '\n'
      if self.condor_should_transfer_files:    txt += 'should_transfer_files    = '  + self.condor_should_transfer_files + '\n'
      if self.condor_when_to_transfer_output:  txt += 'when_to_transfer_output  = '  + self.condor_when_to_transfer_output + '\n'
      if self.condor_executable:               txt += 'Executable               = '  + self.condor_executable + '\n'
      if self.condor_precmd:                   txt += '+PreCmd                  = "' + self.condor_precmd + '"\n'
      if self.condor_preargs:                  txt += '+PreArguments            = "' + self.condor_preargs + '"\n'
      if self.condor_postcmd:                  txt += '+PostCmd                 = "' + self.condor_postcmd + '"\n'
      if self.condor_postargs:                 txt += '+PostArguments           = "' + self.condor_postargs + '"\n'
      if self.condor_transfer_input_files:     txt += 'transfer_input_files     = '  + ','.join(self.condor_transfer_input_files) + '\n'
      if self.condor_transfer_output_files:    txt += 'transfer_output_files    = '  + ','.join(self.condor_transfer_output_files) + '\n'
      if self.condor_environment:              txt += 'environment              = '  + self.condor_environment + '\n'
      txt += 'stream_output = true\nstream_error = true\n'
      if self.condor_queue:                    txt += 'Queue ' + self.condor_queue + '\n'
      else:                                    txt += 'Queue\n'
      
      try:
         file = open(filename,'w')
         file.write(txt)
         file.close()
      except IOError:
         logger.exception('Could not open or write to file'+filename)
         raise

      logger.debug('Condor Job Submit Config File:\n'+txt)




''' Status Codes for Condor Jobs
   Idle        = 1
   Running     = 2
   Removed     = 3
   Completed   = 4
   Holding     = 5


def get_balsam_job_state(job,condor_state):
   if condor_state == 1: # Condor Idle State Code
      return job.QUEUED
   elif condor_state == 2: # Condor Running State Code
      return job.RUNNING
   elif condor_state == 3: # Condor Removed State Code
      return job.FAILED
   elif condor_state == 4: # Condor Completed State Code
      return job.EXECUTION_FINISHED
   elif condor_state == 5: # Condor Holding State Code
      return job.FAILED
'''

def submit(job,cmd):
   ''' should submit a job to the queue and raise a pre-defined sheduler exception if something fails'''
   logging.info('Submitting Condor job: ' + str(job.pk) )
   logging.debug('Submitting command: ' + cmd)

   if settings.BALSAM_SUBMIT_JOBS:
      try:
         # tar working directory for input transfer
         tarball_input_dir(job)
         # parse cmd to get executable and arguments separate
         exe = cmd.split()[0]
         args = cmd[len(exe):]
         # create the configuration for condor
         config = CondorConfig()
         config.condor_initialdir = job.working_directory
         config.condor_executable = exe
         config.condor_arguments = args
         # write the submit script
         config.write_file(job_submit_filename)

         # run submit
         logger.debug('running submit command')
         stdout = run_subprocess.run_subprocess(settings.BALSAM_SCHEDULER_SUBMIT_EXE + ' ' + job_submit_filename)

         index = stdout.find('cluster')
         if index >= 0:
            cluster = stdout[index+8:].split()[0].replace('\n','').replace('.','').strip()
            job.scheduler_id = int(cluster)
         else:
            raise exceptions.JobSubmitFailed('CondorScheduler failed to get scheduler ID during submit command')
      except run_subprocess.SubprocessNonzeroReturnCode,e:
         raise exceptions.SubmitNonZeroReturnCode('CondorScheduler submit command returned non-zero value. command = "' + command +'", exception: ' + str(e))
      except run_subprocess.SubprocessFailed,e:
         raise exceptions.SubmitSubprocessFailed('CondorScheduler subprocess to run commit command failed with exception: ' + str(e))
   else:
      raise exceptions.JobSubmissionDisabled('CondorScheduler Job submission disabled')

   logger.debug('CondorScheduler Job submission complete')


#-- Submitter: ascinode.hep.anl.gov : <146.139.33.17:9100?sock=2835_d9e1_10> : ascinode.hep.anl.gov
# ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
# 6903.0   jchilders      11/3  20:05   0+00:00:00 I  0   0.0  echo

def get_job_status(job):
   ''' should return a scheduler job state for the job passed '''
   logger.debug('Getting status for job ' + str(job.pk) + ', current state = '+job.state)

   try:
      cq = condor_q.condor_q(settings.BALSAM_SCHEDULER_STATUS_EXE)
      cq_jobs = cq.run(job.scheduler_id)

      if len(cq_jobs) == 0:
         logger.debug('CondorScheduler no jobs found with condor_q, trying condor_history.')
         # job has finished so check history
         ch = condor_history.condor_history(settings.BALSAM_SCHEDULER_HISTORY_EXE)
         ch_jobs = ch.run(job.scheduler_id)
         if len(ch_jobs) == 0:
            raise exceptions.JobStatusNotFound('CondorScheduler No Status Found for ClusterID: ' 
                   + str(clusterId) + ' of job ' + str(job.pk))
         logger.debug('CondorScheduler checking ' + str(len(ch_jobs)) + ' from condor_history')
         # loop jobs are all completed or any failed?
         failed = False
         for ch_job in ch_jobs:
            if ch_job.status.isFAILED():
               failed = True
               break
         if failed:
            return jobstates.JOB_FAILED
         
         return jobstates.JOB_FINISHED
      else:
         logger.debug('CondorScheduler checking out ' + str(len(cq_jobs)) + ' jobs.')
         # loop jobs are all idle? Any Held? ANy running?
         held = False
         running = False
         for cq_job in cq_jobs:
            if cq_job.status.isHELD():
               held = True
               break
            elif cq_job.status.isRUNNING():
               running = True
         if held:
            logger.error('CondorScheduler job ' + str(job.pk) + ' has been held for reason: ' + condor_q.condor_q.analyze(job.scheduler_id))
            return jobstates.JOB_HELD
         elif running:
            return jobstates.JOB_RUNNING
         
         return jobstates.JOB_QUEUED


      # now I have the job status, convert to the global 'job state'
      logger.debug(' updated status of job ' + str(job.id) + ' to ' + job.state)

   except run_subprocess.SubprocessFailed,e:
      raise JobStatusFailed('subprocess failed: ' + str(e))
   except run_subprocess.SubprocessNonzeroReturnCode,e:
      raise JobStatusFailed('subprocess returned non-zero return code: ' + str(e))

def postprocess(job):
   ''' untar the output from condor '''
   untar_output(job)

def tarball_input_dir(job):
   ''' tarball the contents of the input directory as the input for the job.
       I do this because condor does not copy subdirectories automatically
       and I wanted to avoid the user specifying all their files since this
       seems to be a condor pequliarity.'''
   logger.debug('in tarball_input_dir')
   cmd = 'tar zcf ' + input_tarball_filename + ' -C ' + job.working_directory + ' .'
   run_subprocess.run_subprocess(cmd)
   shutil.move(input_tarball_filename,job.working_directory+'/')
   logger.debug('tarball_input_dir done')

def untar_output(job):
   ''' after a condor job runs and exits, the output will need to be untarred '''
   logger.debug('in untar_output')
   cmd = 'tar zxf ' + os.path.join(job.working_directory,output_tarball_filename) + ' -C ' + job.working_directory
   run_subprocess.run_subprocess(cmd)
   logger.debug('untar_output done')



'''
def status(joblist):
   logger.debug(" Retrieving status for a list of jobs")
   
   error_list = []
   error_found = False
   for job in joblist:
      error = get_job_status(job)
      error_list.append(error)
      if error != JobErrorCode.NoError:
         error_found = True
   
   #if error_found:
   #   return JobErrorCode.ErrorOccurred,error_list
   
   #return JobErrorCode.NoError,error_list

def get_queue_state(queue_name):
   logger.warning(' CondorScheduler.get_queue_state not yet implemented')
   class QueueState:
       pass
   queue_state = QueueState()
   queue_state.max_size = 0
   return queue_state


def submit_job_with_file(job):
   logger.debug('Submitting Condor Job using file')
   # create job config file
   create_job_submit_file(job)

   working_dir = str(job.working_directory)
   submit_filename = os.path.join(working_dir,job_submit_filename)
   if settings.BALSAM_SUBMIT_JOBS:
      try:
         logger.debug('running submit exe: ' + condor_submit_exe + ' ' + submit_filename)
         p = subprocess.Popen([condor_submit_exe,submit_filename],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      except OSError,e:
         logger.info('Submit of condor job failed ' + str(e))
         return JobErrorCode.JobFailed
      except ValueError,e:
         logger.info('Submit of condor job failed ' + str(e))
         return JobErrorCode.JobFailed
      # pass along the information about this process incase it takes some time
      job.submit_subprocess = p
      
   else:
      logger.info('BALSAM not submitting jobs')
      job.state = job.EXECUTTION_FINISHED
      return JobErrorCode.SubmitNoAllowed
   

   return JobErrorCode.NoError





def submit_job_with_classAd(job):
   logger.debug('Submitting job using python condor modules')
   job_ad = create_classAd(job)
   logger.debug(' Class AD created ')

   if settings.BALSAM_SUBMIT_JOBS:
      schedd = CondorSchedd() # get scheduler
      # list will be filled by ads that are created by submit function
      submitted_ads = []
      cluster_id = schedd.submit(job_ad,1,False,submitted_ads) # only submit job 1 time
      if cluster_id > 0:
         submitted_ad = submitted_ads[0] # since only one job submitted, should only be one Ad
         #job.state = job.CREATED
         logger.debug('job submitted, cluster_id = ' + str(cluster_id))
         job.scheduler_id = submitted_ads[0]['ClusterId']
      else:
         job.message = 'cluster id > 0'
         logger.debug('job failed, cluster_id = ' + str(cluster_id))
         return JobErrorCode.InternalSchedulerError
   else:
      logger.debug('Not submitting job, job submission disabled')
      return JobErrorCode.SubmitNotAllowed
   
   return JobErrorCode.NoError


def cat(filename):
   try:
      file = open(filename,'r')
      logger.info(' cat ' + filename )
      for line in file:
         logger.info(line)
   except:
      logger.warning('failed to cat ' + filename)


def create_classAd(job):
   logger.debug('Creating Condor Class AD')
   
   conf = CondorConfig(job)

   # create empty class ad
   try:
      ad = CondorAd()
      # and fill it with the parameters defined for this job
      if conf.condor_universe:                 ad['Universe']             = conf.condor_universe
      if conf.condor_notifications:            ad['JobNotification']      = conf.condor_notification
      if conf.condor_executable:               ad['Cmd']                  = conf.condor_executable
      if conf.condor_arguments:                ad['Arguments']            = conf.condor_arguments
      if conf.condor_priority:                 ad['JobPrio']              = conf.condor_priority
      if conf.condor_rank:                     ad['Rank']                 = conf.condor_rank
      if conf.condor_requirements:             ad['Requirements']         = conf.condor_requirements
      if conf.condor_environment:              ad['Env']                  = conf.condor_environment
      if conf.condor_initialdir:               ad['Iwd']                  = conf.condor_initialdir
      if conf.condor_input:                    ad['In']                   = conf.condor_input
      if conf.condor_output:                   ad['Out']                  = conf.condor_output
      if conf.condor_error:                    ad['Err']                  = conf.condor_error
      if conf.condor_log:                      ad['UserLog']              = conf.condor_log
      if conf.condor_should_transfer_files:    ad['ShouldTransferFiles']  = conf.condor_should_transfer_files
      if conf.condor_when_to_transfer_output:  ad['WhenToTransferOutput'] = conf.condor_when_to_transfer_output
      if conf.condor_transfer_input_files:     ad['TransferInput']        = conf.condor_transfer_input_files
      if conf.condor_transfer_output_files:    ad['TransferOutput']       = conf.condor_transfer_output_files
      if conf.condor_precmd:                   ad['PreCmd']               = conf.condor_precmd
      if conf.condor_preargs:                  ad['PreArguments']         = conf.condor_preargs
      if conf.condor_postcmd:                  ad['PostCmd']              = conf.condor_postcmd
      if conf.condor_postargs:                 ad['PostArguments']        = conf.condor_postargs
   except:
      logger.exception(' received exception while building the Condor Ad: ' + str(sys.exc_info()[1]))
   logger.debug(str(ad))
   return ad


def get_job_finished_state(job):
   condor_history_exe = 'condor_history'
   if job.scheduler_id:
      clusterId = str(job.scheduler_id)
      
      # call condor_history
      try:
         p = subprocess.Popen([condor_history_exe,clusterId],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      except OSError,e:
         logger.exception('Error calling condor_history'+ str(e))
      except ValueError,e:
         logger.exception('Error calling condor_history'+ str(e))
      
      # get stdout and stderr
      out,err = p.communicate()
      lines = out.split('\n')
      if len(lines) > 3:
         logger.info('CondorScheduler.get_job_finished_state: Something strange is going on. Retrieved more than one history:\n'+out)
      # only care about second line and the first word should be the cluster id
      line = lines[1]
      words = line.split()
      history_cluster_id = words[0].split('.')[0]
      if int(history_cluster_id) != int(clusterId):
         logger.info('CondorScheduler.get_job_finished_state: Something is strange. The cluster id is mismatched, from job: '+clusterID+'; from condor_history: '+history_cluster_id)
         return job.REJECTED
      history_job_finished_state = words[5]
      
      if history_job_finished_state == 'C':
         return job.EXECUTION_FINISHED
      
      return job.FAILED

def setup_condor_for_exe_with_mpi(job,conf):
   conf.condor_executable = settings.BALSAM_ALLOWED_EXECUTABLE_DIRECTORY + '/mpirun.py'

   if conf.condor_transfer_input_files:
      conf.condor_transfer_intput_files += ',' + settings.BALSAM_ALLOWED_EXECUTABLE_DIRECTORY + '/' +  str(job.executable)
   else:
      conf.condor_transfer_intput_files = settings.BALSAM_ALLOWED_EXECUTABLE_DIRECTORY + '/' +  str(job.executable)


def presubmit(job):
   pass

def postsubmit(job):
   pass


def convert_file_list(file_list_text):
   txt = ''
   file_list = json.loads(file_list_text)
   if file_list is not None:
      i = 0
      while i < len(file_list):
         file = str(file_list[i])
         if i > 0:
            txt += ','
         txt += file
         i += 1
   return txt
'''
