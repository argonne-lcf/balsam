# errors from Transition function
TransitionComplete                        = 10
TransitionDbConnectionFailed              = 11
TransitionDbRetrieveFailed                = 12
TransitionFunctionException               = 13
# errors from JobStatusReceiver
JobStatusReceiverRetrieveArgoSubJobFailed = 21
JobStatusReceiverRetrieveArgoJobFailed    = 22
JobStatusReceiverBalsamStateMapFailure    = 23
JobStatusReceiverCompleted                = 24
JobStatusReceiverMessageNoBody            = 25
JobStatusReceiverFailed                   = 26
msg_codes = {
   0:'NoMessageCode',
   TransitionComplete:'TransitionComplete',
   TransitionDbConnectionFailed:'TransitionDbConnectionFailed',
   TransitionDbRetrieveFailed:'TransitionDbRetrieveFailed',
   TransitionFunctionException:'TransitionFunctionException',
   JobStatusReceiverRetrieveArgoSubJobFailed:'JobStatusReceiverRetrieveArgoSubJobFailed',
   JobStatusReceiverRetrieveArgoJobFailed:'JobStatusReceiverRetrieveArgoJobFailed',
   JobStatusReceiverBalsamStateMapFailure:'JobStatusReceiverBalsamStateMapFailure',
   JobStatusReceiverCompleted:'JobStatusReceiverCompleted',
   JobStatusReceiverMessageNoBody:'JobStatusReceiverMessageNoBody',
   JobStatusReceiverFailed:'JobStatusReceiverFailed',
}

class QueueMessage:
   ''' a message used to communicate with the balsam_service main loop '''


   def __init__(self,pk=0,code=0,message=''):
      self.pk = pk
      self.code = code
      self.message = message

   def __str__(self):
      s = ''
      s = '%i:%s:%s' % (self.pk,self.msg_codes[self.msg_code],self.message)
      return s