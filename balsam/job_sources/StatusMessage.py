import logging
logger = logging.getLogger(__name__)


class StatusMessage:
   NO_MESSAGE        = 0x0
   SUCCEEDED         = 0x1 << 0
   SUBMIT_DISABLED   = 0x1 << 1
   FAILED            = 0x1 << 2
   INVALID_EXE       = 0x1 << 3

   message_list = [
                   NO_MESSAGE,
                   SUCCEEDED,
                   SUBMIT_DISABLED,
                   FAILED,
                   INVALID_EXE,
                  ]
   message_text = {
                   NO_MESSAGE:'no message',
                   SUCCEEDED:'succeeded',
                   SUBMIT_DISABLED:'submission disabled',
                   FAILED:'failed',
                   INVALID_EXE:'invalid executable',
                  }


   def __init__(self):
      self.message = StatusMessage.NO_MESSAGE

   def __str__(self):
      out = []
      for message in StatusMessage.message_list:
         if message & self.message:
            out.append(StatusMessage.message_text[message])
      return str(out)

   def contains(self,flag):
      if flag in StatusMessage.message_list:
         if self.message & flag:
            return True
      else:
         logger.warning('Invalid Flag passed')
      return False
