import os,sys,time,multiprocessing,Queue,logging
logger = logging.getLogger(__name__)

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from common import MessageInterface,transfer,DirCleaner
from common import TransitionJob,log_uncaught_exceptions
from argo import UserJobReceiver,JobStatusReceiver,models,QueueMessage

# assign this function to the system exception hook
sys.excepthook = log_uncaught_exceptions.log_uncaught_exceptions

class Command(BaseCommand):
    help = 'Starts ARGO Service to receive user jobs and submit these jobs to Balsam sites'
    logger.info('''
    
    >>>>>   Starting ARGO Service <<<<<
    >>>>>    pid: ''' + str(os.getpid()) + '''      <<<<<

    ''')

    def handle(self, *args, **options):
         subprocesses = {}

         argo_service_queue = multiprocessing.Queue()

         logger.debug(' Launching message queue receiver')
         try:
             p = UserJobReceiver.UserJobReceiver(process_queue=argo_service_queue)
             p.start()
             subprocesses['UserJobReceiver'] = p
         except Exception,e:
             logger.exception(' Received Exception while trying to start job receiver: ' + str(e))
             raise
         logger.debug(' Launching balsam job status receiver ')
         try:
             p = JobStatusReceiver.JobStatusReceiver(process_queue=argo_service_queue)
             p.start()
             subprocesses['JobStatusReceiver'] = p
         except Exception,e:
             logger.exception(' Received exception while trying to start balsam job status receiver: ' + str(e))
             raise

         # setup timer for cleaning the work folder of old files
         logger.debug('creating DirCleaner object')
         workDirCleaner = DirCleaner.DirCleaner(settings.ARGO_WORK_DIRECTORY,
                                     settings.ARGO_DELETE_OLD_WORK_PERIOD,
                                     settings.ARGO_DELETE_OLD_WORK_AGE,
                                    )

         # dictionary by job.pk for each transition subprocess
         jobs_in_transition_by_id = {}

         while True:
            logger.debug('start argo_service loop')

            # first loop over jobs in transition and remove entries that are complete
            logger.debug('checking jobs in transition')
            for pk in jobs_in_transition_by_id.keys():
               proc = jobs_in_transition_by_id[pk]
               if not proc.is_alive():
                  # did subprocess exit cleanly with exitcode == 0
                  if proc.exitcode != 0:
                     logger.error('transition subprocess for  pk=' + str(pk) 
                        + ' returned exit code ' + str(proc.exitcode))
                     # probably want to do other things to recover from error?
                  proc.join()
                  del jobs_in_transition_by_id[pk]

            # see if any jobs are ready to transition, but exclude jobs already in transition
            transitionable_jobs = models.ArgoJob.objects.filter(state__in=models.TRANSITIONABLE_STATES).exclude(pk__in=jobs_in_transition_by_id.keys())
            logger.debug( ' found ' + str(len(transitionable_jobs)) + ' in states that need to be transitioned ')
            # loop over jobs and transition
            for job in transitionable_jobs:
               if len(jobs_in_transition_by_id) < settings.ARGO_MAX_CONCURRENT_TRANSITIONS:
                  logger.debug(' creating job transition ')
                  proc = TransitionJob.TransitionJob(
                           job.pk,
                           argo_service_queue,
                           models.ArgoJob,
                           models.STATES_BY_NAME[job.state].transition_function
                        )
                  logger.debug(' start ')
                  proc.start()
                  jobs_in_transition_by_id[job.pk] = proc
               else:
                  logger.debug(' too many jobs currently transitioning ' 
                     + str(len(jobs_in_transition_by_id)) + ' and max is ' 
                     + str(settings.ARGO_MAX_CONCURRENT_TRANSITIONS))

            # loop over running process and check status
            for name,proc in subprocesses.iteritems():
               if not proc.is_alive():
                  logger.info(' subprocess ' + name + ' has stopped with returncode ' + str(proc.exitcode) )

            # get messages from the queue which tell which job id should be updated to which state
            try:
               logger.info(' waiting for next message, timeout set to ' + str(settings.ARGO_SERVICE_PERIOD) + ' seconds.')
               qmsg = argo_service_queue.get(block=True,timeout=settings.ARGO_SERVICE_PERIOD)
               logger.debug('Received queue message code: ' + str(qmsg.code) + ' = ' + QueueMessage.msg_codes[qmsg.code])
               logger.debug('Received queue message: ' + qmsg.message)
               if qmsg.code == QueueMessage.TransitionComplete:
                  logger.debug('Transition Succeeded')
               elif qmsg.code == QueueMessage.TransitionDbConnectionFailed:
                  logger.error('Transition DB connection failed: ' + qmsg.message)
                  job = models.ArgoJob.objects.get(pk=qmsg.pk)
                  job.state = models.STATE_BY_NAME[job.state].failed_state.name
                  job.save(update_fields=['state'])
               elif qmsg.code == QueueMessage.TransitionDbRetrieveFailed:
                  logger.error('Transition failed to retrieve job from DB: ' + qmsg.message)
                  job = models.ArgoJob.objects.get(pk=qmsg.pk)
                  job.state = models.STATE_BY_NAME[job.state].failed_state.name
                  job.save(update_fields=['state'])
               elif qmsg.code == QueueMessage.TransitionFunctionException:
                  logger.error('Exception received while running transition function: ' + qmsg.message)
                  job = models.ArgoJob.objects.get(pk=qmsg.pk)
                  job.state = models.STATES_BY_NAME[job.state].failed_state.name
                  job.save(update_fields=['state'])
               else:
                  logger.error('Unrecognized QueueMessage code: ' + str(qmsg.code))
            except Queue.Empty:
               logger.debug(' no objects on queue ')

            
            # clean work directory periodically
            if settings.ARGO_DELETE_OLD_WORK:
               workDirCleaner.clean() 
            
         for key,item in receivers.iteritems():
             item.terminate()
             item.join()


         
         logger.debug(' ARGO Service Exiting ')
    


