from common_core.MessageInterface import MessageInterface
from balsam_core.BalsamJobMessage import BalsamJobMessage
from balsam_core.job_sources.StatusMessage import StatusMessage
import logging,time,sys
logger = logging.getLogger(__name__)

class NoMoreJobs(Exception): pass

class JobListener:
   ''' opens message interface and receives jobs '''

   def __init__(self,site_name):
      self.site_name = site_name

      self.msgInt = MessageInterface()
      
      # open connection that just stays open until class is destroyed
      self.msgInt.open_blocking_connection()

      # make sure machine queue exists
      self.msgInt.create_queue(self.site_name,self.site_name)

   def get_job_to_submit(self):
      # create empty message
      msg = BalsamJobMessage()
      # request message
      method,properties,body = self.msgInt.receive_msg(self.site_name)
      if method is not None:
         # acknowledge receipt of the message
         self.msgInt.send_ack(method.delivery_tag)
         logger.debug('BalsamMsgHandler.recv_new_job: received message')
         try:
            msg.load(body)
         except:
            logger.error( ' received exception while loading message body into BalsamJobMessage: ' + str(sys.exc_info()[1]))
            raise
         logger.debug(str(msg))
         return msg
      else:
         logger.debug('No new job message received')
         raise NoMoreJobs('No jobs available')
      return None

   def get_jobs_to_submit(self):
      jobs = []
      i = 0
      # only get 10 jobs at a time so as not to overwhelm the system
      while i < 10:
         try:
            new_job = self.get_job_to_submit()
         except NoMoreJobs,e:
            logger.debug('Done retrieving jobs')
            break
         jobs.append(new_job)
      logger.debug('retrieved ' + str(len(jobs)) + ' jobs to process.')
      return jobs


def send_job_failed(machine_name,job_id,message=None):
   logger.debug(' sending job failed message ')
   msg = StatusMessage.FAILED
   if message is not None:
      msg = (StatusMessage.FAILED | message)
   send_job_status_msg(machine_name,operation = '',job_id=job_id,message=msg)

def send_job_finished(machine_name,job_id,message=None):
   logger.debug(' sending job succeeded message ')
   msg = StatusMessage.SUCCEEDED
   if message is not None:
      msg = (StatusMessage.SUCCEEDED | message)
   send_job_status_msg(machine_name,operation='',job_id=job_id,message=msg)


def send_job_status_msg(machine_name,
                        operation,
                        job_id,
                        message = '',
                        priority = 0, # make message persistent
                        delivery_mode = 2, # default
                       ):
   logger.debug('sending job status message: ' + str(message))

   timestamp = time.time()
   
   # create message interface
   msgInt = MessageInterface()
   msgInt.open_blocking_connection()
   
   # create a header
   headers = {
              'hpc':        machine_name,
              'taskID':     job_id,
              'operation':  operation,
              'created':    int(timestamp),
             }

   # send message
   msgInt.send_msg(str(message),
                     str(job_id),
                     exchange_name = None,
                     message_headers = headers,
                  )
   
   # close connection
   msgInt.close()


