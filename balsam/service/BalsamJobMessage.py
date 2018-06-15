from balsam.common import Serializer

class BalsamJobMessage:
   ''' This is the template for the messages passed to Balsam to create a job '''
   def __init__(self):
      site                 = ''

      name                 = ''
      description          = ''
      origin_id            = ''

      queue                = ''
      project              = ''
      wall_time_minutes    = 0
      num_nodes            = 0
      processes_per_node   = 0
      scheduler_config     = ''
      
      application          = ''
      config_file          = ''

      state                = ''

      input_url            = ''
      output_url           = ''

   def serialize(self):
      return Serializer.serialize(self.__dict__)

   @staticmethod
   def deserialize(text):
      m = BalsamJobMessage()
      m.__dict__ = Serializer.deserialize(text)
      return m
