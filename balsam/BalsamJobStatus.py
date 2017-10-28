
import logging,sys
logger = logging.getLogger(__name__)
from common import Serializer

class SerializeFailed(Exception): pass
class DeserializeFailed(Exception): pass

class BalsamJobStatus:
   def __init__(self,job=None,message=None):
      '''Constructed with a BalsamJob, but only contains simple id,
      serialized_job, and message attributes'''
      self.job_id          = None
      self.serialized_job  = None
      self.message         = message
      if job is not None:
         self.set_job(job)

   def set_job(self,job):
      self.job_id = job.job_id
      try:
         self.serialized_job = job.serialize()
      except Exception as e:
         logger.exception('serialize failed: ' + str(job.__dict__))
         raise SerializeFailed('Received exception while serializing BalsamJob')

   def get_job(self,job):
      if self.serialized_job is not None:
         try:
            job.deserialize(self.serialized_job)
            return job
         except Exception as e:
            logger.exception('deserialize failed: ' + str(self.serialized_job))
         raise DeserializeFailed('Received exception while deserializing BalsamJob')
      return None

   def serialize(self):
      try:
         return Serializer.serialize(self.__dict__)
      except Exception as e:
         logger.exception('serialize failed: ' + str(self.__dict__))
         raise SerializeFailed('Received exception while serializing BalsamJobStatus: ' + str(e))

   def deserialize(self,text):
      try:
         self.__dict__ = Serializer.deserialize(text)
         self.job_id = int(str(self.job_id))
      except Exception as e:
         logger.exception('deserialize failed')
         raise DeserializeFailed('Received exception while deserializing BalsamJobStatus: ' + str(e))
