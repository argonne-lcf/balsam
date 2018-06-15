import sys,logging,json
from BalsamJob import BalsamJob
import Serializer
logger = logging.getLogger(__name__)

class ArgoJob:
   def __init__(self,
                preprocess                = None,
                preprocess_args           = None,
                postprocess               = None,
                postprocess_args          = None,
                input_url                 = None,
                output_url                = None,
                username                  = None,
                email_address             = None,
                group_identifier          = None,
                job_status_routing_key    = None,
               ):
      
      self.preprocess               = preprocess
      self.preprocess_args          = preprocess_args
      self.postprocess              = postprocess
      self.postprocess_args         = postprocess_args
      self.input_url                = input_url
      self.output_url               = output_url
      self.username                 = username
      self.email_address            = email_address
      self.group_identifier         = group_identifier
      self.job_status_routing_key   = job_status_routing_key
      self.jobs               = []

   def add_job(self,job):
      if isinstance(job,BalsamJob):
         self.jobs.append(job)
      else:
         logger.error(' Only jobs of the BalsamJob class can be added to this list. ')
         raise Exception(' JobTypeError ')

   def get_jobs_dictionary_list(self):
      # convert job objects into json strings within the list
      tmp_jobs = []
      for job in self.jobs:
         tmp_jobs.append(job.__dict__)
      return tmp_jobs

   def get_job_list_text(self):
      return Serializer.serialize(self.get_jobs_dictionary_list())
      
   def serialize(self):
      # create temp ArgoJob and fill with this list of json job strings
      tmp_argojob = ArgoJob()
      tmp_argojob.__dict__ = self.__dict__.copy()
      tmp_argojob.jobs = self.get_jobs_dictionary_list()
      
      try:
         return Serializer.serialize(tmp_argojob.__dict__)
      except:
         logger.exception(' received exception while converting ArgoJob to json string: ' + str(sys.exc_info()[1]))
         raise

   def deserialize(self,text_string):
      # fill self with json dictionary
      try:
         self.__dict__ = Serializer.deserialize(text_string)
      except:
         logger.exception(' received exception while converting json string to ArgoJob: ' + str(sys.exc_info()[1]))
         raise

      # convert unicode strings to strings
      self.preprocess                  = Serializer.convert_unicode_string(self.preprocess)
      self.preprocess_args             = Serializer.convert_unicode_string(self.preprocess_args)
      self.postprocess                 = Serializer.convert_unicode_string(self.postprocess)
      self.postprocess_args            = Serializer.convert_unicode_string(self.postprocess_args)
      self.input_url                   = Serializer.convert_unicode_string(self.input_url)
      self.output_url                  = Serializer.convert_unicode_string(self.output_url)
      self.username                    = Serializer.convert_unicode_string(self.username)
      self.group_identifier            = Serializer.convert_unicode_string(self.group_identifier)
      self.job_status_routing_key      = Serializer.convert_unicode_string(self.job_status_routing_key)

      # need to convert vector of json job strings to objects
      tmp_jobs = []
      for job_dictionary in self.jobs:
         tmp_job = BalsamJob()
         tmp_job.__dict__ = job_dictionary
         tmp_jobs.append(tmp_job)
      # now copy into job list
      self.jobs = tmp_jobs
     
