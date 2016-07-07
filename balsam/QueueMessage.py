TransitionComplete = 10
TransitionDbConnectionFailed = 11
TransitionDbRetrieveFailed = 12
TransitionFunctionException = 13
msg_codes = {
   0:'NoMessageCode',
   TransitionComplete:'TransitionComplete',
   TransitionDbConnectionFailed:'TransitionDbConnectionFailed',
   TransitionDbRetrieveFailed:'TransitionDbRetrieveFailed',
   TransitionFunctionException:'TransitionFunctionException',
}

class QueueMessage:
   ''' a message used to communicate with the balsam_service main loop '''
   

   def __init__(self,pk=0,code=0,message=''):
      self.pk = pk
      self.code = code
      self.message = message

   def __str__(self):
      s = ''
      s = '%i:%s:%s' % (self.pk,msg_codes[self.msg_code],self.message)
      return s