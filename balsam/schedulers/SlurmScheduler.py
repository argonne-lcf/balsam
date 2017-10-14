from django.conf import settings
import subprocess,sys,shlex
import os,traceback
import logging
from JobErrorCode import JobErrorCode
from datetime import datetime

logger = logging.getLogger(__name__)



def presubmit(job):
    # - should catch errors that occur during execution
    if job.preprocess is not None:
       cwd = os.getcwd()
       os.chdir( job.working_directory )
       presubmit_script = os.path.join( settings.BALSAM_ALLOWED_EXECUTABLE_DIRECTORY, job.preprocess )
       if os.path.exists( presubmit_script ):
           command = presubmit_script
           if job.preprocess_arguments is not None:
               command += ' ' + str(job.preprocess_arguments)
           logger.debug("Executing presubmit command '%s' from folder '%s'",command,os.getcwd())
           os.system( command )
       else:
           logger.debug("Presubmit script does not exist: %s", presubmit_script)
       os.chdir(cwd)

def submit(job):
    logging.info("Submitting job: %d (%d)", job.id, job.originating_source_id)
    
    # add job specific scheduler options
    if job.options is None or job.options == 'None':
      job.options = ''
    
    command = '%s --account=%s --partition=%s --nodes=%d --ntasks-per-node=%d --time=00:%d:00 --workdir=%s  %s %s %s' % (settings.BALSAM_SCHEDULER_SUBMIT_EXE, 
                job.project,
                job.queue,
                job.num_nodes,
                job.mpi_ranks_per_node,
                job.queue_time_minutes,
                job.working_directory,
                job.options,
                job.executable,
                job.arguments)
    #command = command.replace('"','////"')
    logger.debug('command=%s', command)
    if settings.BALSAM_SUBMIT_JOBS:
        # output = subprocess.check_call( command.split() )
        try:
           p1 = subprocess.Popen( command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
           output,err = p1.communicate()
        except:
           logger.error('command to submit job failed.\n Exception:\n' + traceback.format_exc() )
           #job.state = job.FAILED
           return JobErrorCode.JobFailed
        logger.debug('output = %s', output)
        if 'Submitted batch job' in output:
            job.scheduler_id = int(output.split()[-1])
            logger.debug('job ' + str(job.id) + ' submitted to scheduler as job ' + str(job.scheduler_id))
        
        else:
            logger.error(' job submision failed return code = ' + str(p1.returncode) + '\n stdout:\n ' + output + ' stderr:\n ' + err)
            job.message = stdout + ' ' + err
            return JobErrorCode.JobFailed
        #job.save()
    else:
        logger.info('Not submitting job, job submission disabled')
        #job.state = job.EXECUTION_FINISHED
        #job.save()
        return JobErrorCode.SubmitNotAllowed
    
    logger.debug('Job submission complete')
    return JobErrorCode.NoError

def postsubmit(job):
    # - should catch errors that occur during execution
    if job.postprocess is not None:
      cwd = os.getcwd()
      os.chdir( job.working_directory )
      postsubmit_script = os.path.join( settings.BALSAM_ALLOWED_EXECUTABLE_DIRECTORY, job.postprocess )
      if os.path.exists( postsubmit_script ):
        command = postsubmit_script
        if job.postprocess_arguments is not None:
            command += ' ' + str(job.postprocess_arguments)

        logger.debug("Executing postsubmit command '%s' from folder '%s' ", command,os.getcwd())
        os.system( command )
      else:
        logger.debug("Postsubmit script does not exist: %s", postsubmit_script)
      os.chdir(cwd)

def status(joblist):
    try:
        command = settings.BALSAM_SCHEDULER_STATUS_EXE
        # output = subprocess.check_call( command.split() )
        p1 = subprocess.Popen( command.split(), stdout=subprocess.PIPE)
        output = p1.communicate()[0]
        #print 'output = ', output
        return output

    except:
        logger.exception('Error in update_status')
    logger.info('Leaving update_status')



def get_job_status(job):

    cmd = settings.BALSAM_SCHEDULER_STATUS_EXE + ' -j ' + str(job.scheduler_id) + ' -o state -n'
    try:
       p = subprocess.Popen(shlex.split(cmd),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    except Exception as e:
       logger.error(' exception while trying get job status for job id = ' + job.originating_source_id + ': ' + str(e))
       raise


    stdout,stderr = p.communicate()

        
    lines = stdout.split('\n')
    if len(lines) >= 1:
       state = lines[0].strip()
       logger.debug(' job ' + str(job.id) + ' with scheduler id ' + str(job.scheduler_id) + ' is in state ' + str(state))
       # PENDING, RUNNING, SUSPENDED, CANCELLED, COMPLETING, COMPLETED, CONFIGURING, FAILED, TIMEOUT, PREEMPTED, NODE_FAIL and SPECIAL_EXIT.  See the JOB STATE CODES  section  below for more information.  (Valid for jobs only)
       if state in ('COMPLETED'):
          job.state = job.EXECUTION_FINISHED
          return JobErrorCode.NoError
       elif state in ('PENDING'):
          job.state = job.QUEUED
          return JobErrorCode.NoError
       elif state in ('RUNNING','SUSPENDED','COMPLETING','CONFIGURING'):
          job.state = job.RUNNING
          return JobErrorCode.NoError

    job.state = job.FAILED
    return JobErrorCode.NoError


def get_queue_state(queue_name):
   pass
         
