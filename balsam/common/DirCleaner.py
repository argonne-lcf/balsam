from balsam.common.file_tools import delete_old_files_directories
import time

class DirCleaner:
   def __init__(self,path,period_in_sec,cutoff_in_seconds,remove_files = True, remove_directories = True):
      self.start_time = time.time()
      self.path = path
      self.period = period_in_sec
      self.last_time = self.start_time
      self.cutoff = cutoff_in_seconds
      self.remove_files = remove_files
      self.remove_directories = remove_directories

   def clean(self):
      current_time = time.time()
      if current_time - self.last_time > self.period:
         delete_old_files_directories(self.path,
                                      self.cutoff,
                                      self.remove_files,
                                      self.remove_directories)
