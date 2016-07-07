import os,sys,logging
logger = logging.getLogger(__name__)
from common import run_subprocess
# ID     OWNER          SUBMITTED   RUN_TIME     ST COMPLETED   CMD
# 6904.0   jchilders      11/3  20:46   0+00:00:00 X         ???  /bin/echo hello there
# 6905.0   jchilders      11/3  21:16   0+00:00:01 C  11/3  21:16 /bin/echo hello there

class condor_history:
   
   class status:
      FAILED = 'X'
      COMPLETED = 'C'
      STATES = [FAILED,COMPLETED]
      def __init__(self,status_string):
         self.stat = None
         for entry in self.STATES:
            if status_string == entry:
               self.stat = entry

               
      def __eq__(self,rhs):
         if self.stat == rhs.stat:
            return True
         return False
      def isFAILED(self):
         if self.stat == self.FAILED:
            return True
         return False
      def isCOMPLETED(self):
         if self.stat == self.COMPLETED:
            return True
         return False
   
   class subjob:
      def __init__(self):
         self.cluster_id   = None
         self.subjob_id    = None
         self.owner        = None
         self.submitted    = None
         self.run_time     = None
         self.status       = None
         self.completed    = None
         self.cmd          = None
      @staticmethod
      def from_line(line):
         subjob = condor_history.subjob()
         line = line.replace('\n','').strip()
         words = line.split()
         subjob.cluster_id = int(words[0].split('.')[0])
         subjob.subjob_id  = int(words[0].split('.')[1])
         subjob.owner      = words[1]
         subjob.submitted  = words[2] + ' ' + words[3]
         subjob.run_time   = words[4]
         subjob.status     = condor_history.status(words[5])
         offset = 0
         if subjob.status.isFAILED():
            subjob.completed  = words[6]
         else:
            subjob.completed  = words[6] + ' ' + words[7]
            offset += 1
         subjob.cmd = words[7]
         for word in words[8+offset:]:
            subjob.cmd += ' ' + word
         return subjob

   def __init__(self,exe='/usr/bin/condor_history'):
      self.exe = exe

   def run(self,cluster_id = None):
      logger.debug(' checking condor_history for cluster id: ' + str(cluster_id))
      cmd = self.exe
      if cluster_id: cmd += ' ' + str(cluster_id)
      try:
         stdout = run_subprocess.run_subprocess(cmd)
      except:
         logger.exception(' received exception while trying to run ' + str(self.exe))
         raise
      
      subjob_list = []
      lines = stdout.split('\n')
      for line in lines[1:]:
         if len(line) > 0:
            subjob_list.append(condor_history.subjob.from_line(line))
      
      return subjob_list
