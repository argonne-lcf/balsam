import common.Serializer as Serializer
from django.core import serializers
import logging,sys
logger = logging.getLogger(__name__)

class SerializeFailed(Exception): pass
class DeserializeFailed(Exception): pass

class BalsamJobStatus:
   def __init__(self,job=None,message=None):
      self.id              = None
      self.serialized_job  = None
      self.message         = message
      if job is not None:
         self.set_job(job)

   def set_job(self,job):
      self.id = job.balsam_job_id
      try:
         self.serialized_job = serializers.serialize('json',[job])
      except Exception,e:
         logger.exception('serialize failed: ' + str(job.__dict__))
         raise SerializeFailed('Received exception while serializing BalsamJob')

   def get_job(self):
      if self.serialized_job is not None:
         try:
            balsam_jobs = serializers.deserialize('json',self.serialized_job)
            return balsam_jobs.next().object
         except Exception,e:
            logger.exception('deserialize failed: ' + str(self.serialized_job))
         raise DeserializeFailed('Received exception while deserializing BalsamJob')
      return None

   def serialize(self):
      try:
         return Serializer.serialize(self.__dict__)
      except Exception,e:
         logger.exception('serialize failed: ' + str(self.__dict__))
         raise SerializeFailed('Received exception while serializing BalsamJobStatus: ' + str(e))

   def deserialize(self,text):
      try:
         self.__dict__ = Serializer.deserialize(text)
         self.id = int(str(self.id))
      except Exception,e:
         logger.exception('deserialize failed')
         raise DeserializeFailed('Received exception while deserializing BalsamJobStatus: ' + str(e))
