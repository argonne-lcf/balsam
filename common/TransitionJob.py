import multiprocessing,logging
logger = logging.getLogger(__name__)

from django.db import utils,connections,DEFAULT_DB_ALIAS
from balsam import QueueMessage
from common import db_tools

class TransitionJob(multiprocessing.Process):
   ''' spawns subprocess which finds the DB entry for the given id
       and transitions that entry to the next state. Then it alerts
       the service thread using the message queue given.'''
   def __init__(self,entry_pk,queue,job_base_class,transition_function):
      logger.debug('Creating TransitionJob object for pk=' + str(entry_pk))
      self.entry_pk = entry_pk
      self.queue = queue
      self.job_base_class = job_base_class
      self.transition_function = transition_function
      super(TransitionJob, self).__init__()

   def run(self):
      logger.debug('Running TransitionJob object for pk=' + str(self.entry_pk))
      # create unique DB connection string
      try:
         db_connection_id = db_tools.get_db_connection_id(self.entry_pk)
         db_backend = utils.load_backend(connections.databases[DEFAULT_DB_ALIAS]['ENGINE'])
         db_conn = db_backend.DatabaseWrapper(connections.databases[DEFAULT_DB_ALIAS], db_connection_id)
         connections[db_connection_id] = db_conn
      except Exception as e:
         self.queue.put(QueueMessage.QueueMessage(self.entry_pk,QueueMessage.TransitionDbConnectionFailed,
                   'Failed to get local connection to DB. Exception: ' + str(e)))
         return
      
      logger.debug('DB connection created pk=' + str(self.entry_pk))
      
      # retreive job from DB
      try:
         job = self.job_base_class.objects.get(pk=self.entry_pk)
      except Exception as e:
         self.queue.put(QueueMessage.QueueMessage(self.entry_pk,QueueMessage.TransitionDbRetrieveFailed,
                   'Failed to retrieve job id ' + str(self.entry_pk) + ' from DB for base_class ' + str(self.job_base_class.__name__) + '. Exception: ' + str(e)))
         return
      
      logger.debug('retrieved pk=' + str(self.entry_pk) + ' state='+job.state)
       
      # transition state
      try:
         if self.transition_function is not None:
            logger.debug(' pk='+str(job.pk) + ' state='+job.state + ' transition_function=' + str(self.transition_function.__name__))
            self.transition_function(job)
         else:
            logger.debug(' pk='+str(job.pk) + ' state='+job.state + ' transition_function is None')
         logger.debug(' pk='+str(job.pk) + ' state='+job.state + ' transition_function=' + str(self.transition_function.__name__) + ' completed') 
      except Exception as e:
         message = 'Transition function, '
         if self.transition_function is None:
            message += 'None'
         else:
            message += self.transition_function.__name__
         message += ', for state, '+ job.state +', failed with exception: ' + str(e)
         self.queue.put(QueueMessage.QueueMessage(self.entry_pk,QueueMessage.TransitionFunctionException,message))
         return

      # send message to balsam_service about completion
      self.queue.put(QueueMessage.QueueMessage(self.entry_pk,QueueMessage.TransitionComplete))


      # Transition Done.
