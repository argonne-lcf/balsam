#!/usr/bin/env python
import os,sys,logging
logger = logging.getLogger(__name__)

from common import run_subprocess

#-- Submitter: ascinode.hep.anl.gov : <146.139.33.17:9100?sock=2835_d9e1_10> : ascinode.hep.anl.gov
# ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
# 6903.0   jchilders      11/3  20:05   0+00:00:00 I  0   0.0  echo

class condor_q:
   
   class CondorStatus:
      RUNNING  = 'R'
      IDLE     = 'I'
      HELD     = 'H'
      STATES   = [RUNNING,IDLE,HELD]
      def __init__(self,state_string):
         self.status = None
         for status in self.STATES:
            if state_string == status:
               self.status = status
               return
      def __eq__(self,rhs):
         if self.status == rhs.status:
            return True
         return False
      def __str__(self):
         if self.isRUNNING():
            return 'RUNNING'
         elif self.isIDLE():
            return 'IDLE'
         elif self.isHELD():
            return 'HELD'
         return 'UNKNOWN'
      def isRUNNING(self):
         if self.status == self.RUNNING:
            return True
         return False
      def isIDLE(self):
         if self.status == self.IDLE:
            return True
         return False
      def isHELD(self):
         if self.status == self.HELD:
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
         self.priority     = None
         self.size         = None
         self.cmd          = None
      
      @staticmethod
      def from_line(line):
         subjob = condor_q.subjob()
         line = line.replace('\n','').strip()
         words = line.split()
         subjob.cluster_id = int(words[0].split('.')[0])
         subjob.subjob_id  = int(words[0].split('.')[1])
         subjob.owner      = words[1]
         subjob.submitted  = words[2] + ' ' + words[3]
         subjob.run_time   = words[4]
         subjob.status     = condor_q.CondorStatus(words[5])
         subjob.priority   = words[6]
         subjob.size       = words[7]
         subjob.cmd        = words[8]
         return subjob

   def __init__(self,exe='/usr/bin/condor_q'):
      self.exe = exe

   def run(self,cluster_id = None):
      cmd = self.exe
      if cluster_id: cmd += ' ' + str(cluster_id)
      subjob_list = []
      try:
         stdout = run_subprocess.run_subprocess(cmd)
      except:
         logger.error(' received exception while trying to run ' + str(self.exe))
         raise
      
      lines = stdout.split('\n')
      in_jobs = False
      for line in lines:
         # order matters. want to catch the end before proceeding 
         if 'completed' in line and 'removed' in line and 'idle' in line:
            in_jobs = False
            continue
         elif in_jobs and len(line) > 3:
            subjob_list.append(condor_q.subjob.from_line(line))
            continue
         elif 'ID' in line and 'OWNER' in line and 'CMD' in line:
            in_jobs = True
            continue

      return subjob_list

   @staticmethod
   def analyze(cluster_id,exe = '/usr/bin/condor_q -analyze '):
      cmd = exe + str(cluster_id)
      try:
         stdout = run_subprocess.run_subprocess(cmd)
      except:
         logger.exception(' received exception while trying to run ' + str(cmd))
         raise

      lines = stdout.split('\n')
      for line in lines:
            if line.find('Hold reason:') >= 0:
                  return line
      return ''



         
