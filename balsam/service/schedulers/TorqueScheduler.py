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
    
    
    '''
    hyperthreading = False
    if job.site == 'edison' and job.mpi_ranks_per_node > 24:
      hyperthreading = True
    logger.debug('hyperthreading = ' + str(hyperthreading) + ' site = ' + job.site + ' ranks = ' + str(job.mpi_ranks_per_node))
    
    os.chdir(job.working_directory)
    script_file = os.path.join(job.working_directory,'run.sh')

    aprun_command = 'aprun -n ' + str(job.mpi_ranks_per_node * job.num_nodes)
    if hyperthreading:
      aprun_command += ' -j 2'
    aprun_command += ' ' +  job.executable

    if job.arguments is not None:
      aprun_command += ' ' + str(job.arguments)

    fp = file(script_file,'w')
    print >>fp, '#!/bin/bash'
    print >>fp, aprun_command
    fp.close()
    os.chmod(script_file, 0755)
    
    mppwidth = job.num_nodes * job.mpi_ranks_per_node
    if job.site == 'edison':
      mppwidth = job.num_nodes * 24
   '''
    if job.options is None or job.options == 'None':
      job.options = ''
    
    mppwidth = job.mpi_ranks_per_node * job.num_nodes
    if 'edison' in job.site and job.mpi_ranks_per_node > 24:
        mppwidth = job.num_nodes * 24
    
    command = '%s -A %s -q %s -l mppwidth=%d -l walltime=00:%d:00 -d %s %s %s' % (settings.BALSAM_SCHEDULER_SUBMIT_EXE, 
                job.project,
                job.queue,
                mppwidth,
                job.queue_time_minutes,
                job.working_directory,
                job.options,
                job.executable)
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
        if len(output) == 0:
            logger.debug('Reading job id from stderr thanks to new Cobalt')
            output = err
            output = output.split('.')[0]
        if p1.returncode == 0:
            output = output.strip()
            output = output.split('.')[0]
            logger.debug('job ' + str(job.id) + ' submitted to scheduler as job ' + str(output))
            job.scheduler_id = output
            #job.state = job.QUEUED
        else:
            logger.error(' job failed because return code was non-zero: ' + str(p1.returncode) + '\n stdout:\n ' + output + ' stderr:\n ' + err)
            #job.state = job.FAILED
            job.message = err
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


# -  the job state:
#      C -     Job is completed after having run/
#      E -  Job is exiting after having run.
#      H -  Job is held.
#      Q -  job is queued, eligible to run or routed.
#      R -  job is running.
#      T -  job is being moved to new location.
#      W -  job is waiting for its execution time
#           (-a option) to be reached.
#      S -  (Unicos only) job is suspend.
def get_job_status(job):
    
    qstat = QStat(job.scheduler_id)
    
    # if job is done, qstat doesn't return anything
    if qstat.qstat_line is None:
        logger.debug(' qstat was empty ')
        job.state = job.EXECUTION_FINISHED
        return JobErrorCode.NoError
    
    # parse returned state from qstat
    state = qstat.qstat_line.state
    
    if state in ('R','E'):
        job.state = job.RUNNING
    elif state == 'Q':
        job.state = job.QUEUED

    return JobErrorCode.NoError

def get_queue_state(queue_name):
    '''
    Note: queue_name is ignored for now
    '''
    # - this is rudimentary for now: it determines whether a block of suitable size 
    #   is idle at the moment, in which case the estimate returned is zero. Otherwise, it
    #   returns a large estimate
    class QueueState:
        pass

    # command = 'partlist'
    # p1 = subprocess.Popen( command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # output,err = p1.communicate()
    # outputlines = output.split('\n')[2:-1]
    # #print outputlines
    # Q = [o.split()[:3] for o in outputlines]
    # Q = filter( lambda x: x[2] == 'idle', Q )
    # sizemap = {}
    # for q in Q:
    #     #print q
    #     location = q[0]
    #     siz = location.split('-')[-1]
    #     sizemap[siz] = 1
    # sizes = [int(s) for s in sizemap.keys()]
    queue_state = QueueState()

    # if sizes:
    #     queue_state.max_size = max(sizes)
    # else:
    #     queue_state.max_size = 0
    return queue_state

class QStat:
   def __init__(self,scheduler_id):
      self.qstat_line = None
      qstat_cmd = settings.BALSAM_SCHEDULER_STATUS_EXE + ' ' + str(scheduler_id)

      try:
         p = subprocess.Popen(shlex.split(qstat_cmd),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      except:
         logger.exception(' received exception while trying to run qstat: ' + str(sys.exc_info()[1]))

      stdout,stderr = p.communicate()
      logger.debug(' qstat ouput: \n' + stdout )
      if p.returncode != 0:
         logger.warning(' return code for qstat is non-zero. Likely means job exited. stdout = \n' + stdout + '\n stderr = \n' + stderr )
      
      if len(stdout) > 0:
         lines = stdout.split('\n')
         if len(lines) >= 2:
            self.qstat_line = QStatLine.instanceFromQstatLine(lines[2])
      

# Format of qstat output on Edison is like so:
# Job ID                    Name             User            Time Use S Queue
# ------------------------- ---------------- --------------- -------- - -----
# 2202544.edique02           ...reate_dir_job mistark                0 H reg_small      
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
      qstat_line.job_id    = int(list[0].split('.')[0])
      qstat_line.name      = str(list[1])
      qstat_line.user      = str(list[2])
      qstat_line.time_used = list[3]
      qstat_line.state     = str(list[4])
      qstat_line.queue     = str(list[5])
      return qstat_line
         
