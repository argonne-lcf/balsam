import common.Serializer as Serializer

class ArgoJobStatus:
   def __init__(self):
      self.state       = None
      self.job_id       = None
      self.message      = None

   def get_serialized_message(self):
      return Serializer.serialize(self.__dict__)

   @staticmethod
   def get_from_message(message):
      tmp = ArgoJobStatus()
      tmp.__dict__ = Serializer.deserialize(message)
      return tmp