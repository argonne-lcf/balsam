import sys,logging,copy
import Serializer
logger = logging.getLogger(__name__)


class ArgoUserSubJob:
   def __init__(self,
                site                      = '',
                job_id                    = 0,
                job_name                  = '',
                job_description           = '',
                queue_id                  = 0,
                project_id                = 0,
                wall_time_minutes         = 0,
                num_nodes                 = 0,
                processes_per_node        = 0,
                scheduler_config_id       = 0,
                task_id                   = 0,
                task_input_file           = '',
               ):
      self.site                           = site
      self.job_id                         = job_id
      self.job_name                       = job_name
      self.job_description                = job_description
      self.queue_id                       = queue_id
      self.project_id                     = project_id
      self.wall_time_minutes              = wall_time_minutes
      self.num_nodes                      = num_nodes
      self.processes_per_node             = processes_per_node
      self.scheduler_config_id            = scheduler_config_id
      self.task_id                        = task_id
      self.task_input_file                = task_input_file

   def serialize(self):
      return Serializer.serialize(self.__dict__)
   @staticmethod
   def deserialize(text):
      tmp = ArgoUserSubJob()
      tmp.__dict__ = Serializer.deserialize(text)

      # convert unicode strings to strings
      tmp.site             = Serializer.convert_unicode_string(tmp.site)
      tmp.job_id           = Serializer.convert_unicode_string(tmp.job_id)
      tmp.job_description  = Serializer.convert_unicode_string(tmp.job_description)
      tmp.task_input_file  = Serializer.convert_unicode_string(tmp.task_input_file)

      return tmp
      

class ArgoUserJob:
   def __init__(self,
                job_id                    = 0,
                job_name                  = '',
                job_description           = '',
                group_identifier          = '',

                username                  = '',
                email_address             = '',

                input_url                 = '',
                output_url                = '',

                job_status_routing_key    = '',
                subjobs                   = [],
               ):
      self.job_id                         = job_id
      self.job_name                       = job_name
      self.job_description                = job_description
      self.group_identifier               = group_identifier

      self.username                       = username
      self.email_address                  = email_address

      self.input_url                      = input_url
      self.output_url                     = output_url

      self.job_status_routing_key         = job_status_routing_key
      self.subjobs                        = subjobs
      
   def serialize(self):
      try:
         # loop over sub jobs and serialize each one
         serial_subjobs = []
         for subjob in self.subjobs:
            serial_subjobs.append(subjob.serialize())
         tmp = copy.deepcopy(self)
         tmp.subjobs = serial_subjobs
         return Serializer.serialize(tmp.__dict__)
      except Exception as e:
         raise Exception('Exception received while serializing ArgoUserJob: ' + str(e))

   @staticmethod
   def deserialize(text):
      # fill tmp with json dictionary
      tmp = ArgoUserJob()
      try:
         tmp.__dict__ = Serializer.deserialize(text)
         subjobs = []
         for subjob in self.subjobs:
            tmp = ArgoUserSubJob.deserialize(subjob)
            subjobs.append(tmp)
         tmp.subjobs = subjobs
      except Exception as e:
         raise Exception('Exception received while deserializing ArgoUserJob: ' + str(e))

      # convert unicode strings to strings
      tmp.job_name                    = Serializer.convert_unicode_string(self.job_name)
      tmp.job_description             = Serializer.convert_unicode_string(self.job_description)
      tmp.group_identifier            = Serializer.convert_unicode_string(self.group_identifier)
      tmp.username                    = Serializer.convert_unicode_string(self.username)
      tmp.email_address               = Serializer.convert_unicode_string(self.email_address)
      tmp.input_url                   = Serializer.convert_unicode_string(self.input_url)
      tmp.output_url                  = Serializer.convert_unicode_string(self.output_url)
      tmp.job_status_routing_key      = Serializer.convert_unicode_string(self.job_status_routing_key)

      return tmp

     
