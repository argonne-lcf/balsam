#!/usr/bin/env python
import os,sys,logging
logger = logging.getLogger(__name__)
from common import run_subprocess

# Submitting job(s).
# 1 job(s) submitted to cluster 6923.

class condor_submit:
   class SubmitException:
      def __init__(self,filename):
         self.filename
      def __str__(self):
         return ' Failed to submit ' + filename
   
   def __init__(self,exe='/usr/bin/condor_submit'):
      self.exe = exe

   def run(self,filename):
      cmd = self.exe + ' ' + filename
      try:
         stdout = run_subprocess.run_subprocess(cmd)
      except:
         logger.exception(' received exception while trying to run ' + str(self.exe))
         raise
      
      lines = stdout.split('\n')
      logger.info(' lines = ' + str(lines))
      for line in lines:
         if line.find('job(s) submitted to cluster') > 0:
            return int(line.split()[-1].replace('.','').strip())
      
      raise Exception('Error extracting cluster id from condor_submit')


         
