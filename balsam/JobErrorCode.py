

class JobErrorCode:
   """ Error Codes returned by Job related operations """

   NoError              = 0
   JobNotFound          = 1
   MultipleJobsFound    = 2
   JobFailed            = 3
   SubmitNotAllowed     = 4
   ErrorOccurred        = 5
   InternalSchedulerError = 6
   JobUnsanitary        = 7
   ExecutableInvalid    = 8
   JobHeld              = 9
   PresubmitFailed      = 10

   Names = [
            'NoError',
            'JobNotFound',
            'MultipleJobsFound',
            'JobFailed',
            'SubmitNotAllowed',
            'ErrorOccurred',
            'InternalSchedulerError',
            'JobUnsanitary',
            'ExecutableInvalid',
            'JobHeld',
            'PresubmitFailed',
           ]
   
   def __init(self,code = NoError):
      self.error_code = code

   def __eq__(self,rhs):
      if self.error_code == rhs.error_code:
         return True
      return False

   def __ne__(self,rhs):
      if self.error_code != rhs.error_code:
         return True
      return False

