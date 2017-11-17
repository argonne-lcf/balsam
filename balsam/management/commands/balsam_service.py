import os,sys,logging,multiprocessing,queue,traceback
logger = logging.getLogger(__name__)

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from balsam import models,BalsamJobReceiver,QueueMessage,BalsamStatusSender
from common import DirCleaner,log_uncaught_exceptions,TransitionJob
from balsam import scheduler
from balsam.schedulers import exceptions,jobstates

# assign this function to the system exception hook
sys.excepthook = log_uncaught_exceptions.log_uncaught_exceptions

class Command(BaseCommand):
   help = 'Start Balsam Service, which monitors the message queue for new jobs and submits them to the local batch system.'
   logger.info('''
   >>>>>      Starting Balsam Service                 <<<<<  
   >>>>>      pid: ''' + str(os.getpid()) + '''       <<<<<
      ''')

   def handle(self, *args, **options):

      try:
         
         logger.debug('starting BalsamJobReceiver')
         subprocesses = {}
         # start the balsam job receiver in separate thread
         try:
            p = BalsamJobReceiver.BalsamJobReceiver(settings.RECEIVER_CONFIG)
            p.start()
            subprocesses['BalsamJobReceiver'] = p
         except Exception as e:
             logger.exception(' Received Exception while trying to start job receiver: ' + str(e))
         
         # Balsam status message sender
         status_sender = BalsamStatusSender.BalsamStatusSender(settings.SENDER_CONFIG)

         # setup timer for cleaning the work folder of old files
         logger.debug('creating DirCleaner')
         workDirCleaner = DirCleaner.DirCleaner(settings.BALSAM_WORK_DIRECTORY,
                                     settings.BALSAM_DELETE_OLD_WORK_PERIOD,
                                     settings.BALSAM_DELETE_OLD_WORK_AGE,
                                    )

         # create the balsam service queue which subprocesses use to commicate
         # back to the the service. It is also used to wake up the while-loop
         logger.debug('creating balsam_service_queue')
         balsam_service_queue = multiprocessing.Queue()
         jobs_in_transition_by_id = {}

         # this is the loop that never ends, yes it goes on and on my friends...
         while True:
            logger.debug('begin service loop ')


            # loop over queued jobs and check their status
            # also look for jobs that have been submitted but are not in the queued or running state, which 
            # may mean they have finished or exited.
            logger.debug( ' checking for active jobs ')
            active_jobs = models.BalsamJob.objects.filter(state__in = models.CHECK_STATUS_STATES)
            if len(active_jobs) > 0:
               logger.info( 'monitoring ' + str(len(active_jobs)) + ' active jobs')
            else:
               logger.debug(' no active jobs')

            for job in active_jobs:
               # update job's status
               try:
                  jobstate = scheduler.get_job_status(job)
                  if jobstate == jobstates.JOB_RUNNING and job.state != models.RUNNING.name:
                     job.state = models.RUNNING.name
                  elif jobstate == jobstates.JOB_QUEUED and job.state != models.QUEUED.name:
                     job.state = models.QUEUED.name
                  elif jobstate == jobstates.JOB_FINISHED and job.state != models.EXECUTION_FINISHED.name:
                     job.state = models.EXECUTION_FINISHED.name
                  else:
                     logger.debug('job pk=' + str(job.pk) + ' remains in state ' + str(jobstate))
                     continue # jump to next job, skip remaining actions
                  job.save(update_fields=['state'])
                  status_sender.send_status(job,'Job entered ' + job.state + ' state')
               except exceptions.JobStatusFailed as e:
                  message = 'get_job_status failed for pk='+str(job.pk)+': ' + str(e)
                  logger.error(message)
                  # TODO: Should I fail the job?
                  status_sender.send_status(job,message)
               except Exception as e:
                  message = 'failed to get status for pk='+str(job.pk)+', exception: ' + str(e)
                  logger.error(message)
                  # TODO: Should I fail the job?
                  status_sender.send_status(job,message)



            # first loop over jobs in transition and remove entries that are complete
            # 2-->3 bug: have to cast keys from iterator to list 
            for pk in list(jobs_in_transition_by_id.keys()):
               proc = jobs_in_transition_by_id[pk]
               if not proc.is_alive():
                  # did subprocess exit cleanly with exitcode == 0
                  if proc.exitcode != 0:
                     logger.error('transition subprocess for  pk=' + str(pk) 
                               + ' returned exit code ' + str(proc.exitcode))
                     # probably want to do other things to recover from error?
                  del jobs_in_transition_by_id[pk]
                     

            # see if any jobs are ready to transition, but exclude jobs already in transition
            transitionable_jobs = models.BalsamJob.objects.filter(state__in=models.TRANSITIONABLE_STATES).exclude(pk__in=jobs_in_transition_by_id.keys())
            logger.debug( ' found ' + str(len(transitionable_jobs)) + ' in states that need to be transitioned ')
            # loop over jobs and transition
            for job in transitionable_jobs:
               # place a limit on the number of concurrent threads to avoid overloading CPU
               if len(jobs_in_transition_by_id) < settings.BALSAM_MAX_CONCURRENT_TRANSITIONS:
                  logger.debug(' creating job transition ')
                  proc = TransitionJob.TransitionJob(
                           job.pk,
                           balsam_service_queue,
                           models.BalsamJob,
                           models.STATES_BY_NAME[job.state].transition_function
                        )
                  logger.debug(' starting TransitionJob process ')
                  proc.start()
                  jobs_in_transition_by_id[job.pk] = proc
               else:
                  logger.debug(' too many jobs currently transitioning ' 
                     + str(len(jobs_in_transition_by_id)) + ' and max is ' 
                     + str(settings.BALSAM_MAX_CONCURRENT_TRANSITIONS))

            # clean work directory periodically
            if settings.BALSAM_DELETE_OLD_WORK:
               workDirCleaner.clean()

            # loop over running process and check status
            for name,proc in subprocesses.items():
               if not proc.is_alive():
                  logger.info(' subprocess ' + name + ' has stopped with returncode ' + str(proc.exitcode) )

            # block on getting message from the queue where subprocesses will send messages
            try:
               logger.debug('getting message from queue, blocking for ' 
                            + str(settings.BALSAM_SERVICE_PERIOD) + ' seconds')
               qmsg = balsam_service_queue.get(block=True,timeout=settings.BALSAM_SERVICE_PERIOD)
               # act on messages
               logger.debug('Received queue message code: ' + QueueMessage.msg_codes[qmsg.code])
               logger.debug('Received queue message: ' + qmsg.message)
               if qmsg.code == QueueMessage.TransitionComplete:
                  logger.debug('Transition Succeeded')
               elif qmsg.code == QueueMessage.TransitionDbConnectionFailed:
                  logger.error('Transition DB connection failed: ' + qmsg.message)
                  job = models.BalsamJob.objects.get(pk=qmsg.pk)
                  job.state = models.STATES_BY_NAME[job.state].failed_state
                  job.save(update_fields=['state'])
               elif qmsg.code == QueueMessage.TransitionDbRetrieveFailed:
                  logger.error('Transition failed to retrieve job from DB: ' + qmsg.message)
                  job = models.BalsamJob.objects.get(pk=qmsg.pk)
                  job.state = models.STATES_BY_NAME[job.state].failed_state
                  job.save(update_fields=['state'])
               elif qmsg.code == QueueMessage.TransitionFunctionException:
                  logger.error('Exception received while running transition function: ' + qmsg.message)
                  job = models.BalsamJob.objects.get(pk=qmsg.pk)
                  job.state = models.STATES_BY_NAME[job.state].failed_state
                  job.save(update_fields=['state'])
               else:
                  logger.error('No recognized QueueMessage code')
            except queue.Empty as e:
               logger.debug('no messages on queue')

      
         logger.info(' Balsam Service Exiting ')
      except KeyboardInterrupt as e:
         logger.info('Balsam Service Exiting')
         return

