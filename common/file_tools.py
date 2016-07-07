#!/usr/bin/env python
import os,sys,time,logging,optparse,shutil
logger = logging.getLogger(__name__)
DAY_IN_SECS = 86400

def delete_old_files_directories(path,delete_older_than = 7 * DAY_IN_SECS,remove_files = True, remove_directories = True):
   now = time.time()
   cutoff = now - delete_older_than
   
   if not os.path.exists(path):
      logger.error('Path does not exist: ' + path)
      raise Exception('Path does not exist: ' + path)

   items = os.listdir(path)
   for item in items:
      full_item = os.path.join(path,item)
      if os.path.isfile(full_item) and remove_files:
         t = os.stat(full_item)
         c = t.st_mtime
         
         # delete file if older than a week
         if c < cutoff:
            logger.debug(' removing: ' + full_item)
            os.remove(full_item)
      if os.path.isdir(full_item) and remove_directories:
         t = os.stat(full_item)
         c = t.st_mtime

         # delete file if older than a week
         if c < cutoff:
            logger.debug(' removing: ' + full_item)
            shutil.rmtree(full_item)


def main():
   logging.basicConfig(level=logging.INFO)

   parser = optparse.OptionParser(description='')
   parser.add_option('-d','--delete-old',dest='delete_old',help='Enables calling delete_old_files_directories.',action='store_true',default=False)
   parser.add_option('--path',dest='path',help='Path in which to detele files and folders. Goes with "delete-old" option')
   parser.add_option('--cutoff',dest='cutoff',help='The time before which files and folders will be deleted',type='int')
   parser.add_option('--skip-files',dest='remove_files',help='Do not delete files.',action='store_false',default=True)
   parser.add_option('--skip-directories',dest='remove_directories',help='Do not delete directories.',action='store_false',default=True)
   options,args = parser.parse_args()

   if options.delete_old:
      if options.path is None:
         parser.error('must specify path to delete.')
      if options.cutoff is None:
         parser.error('must specify cutoff to delete.')

      delete_old_files_directories(options.path,options.cutoff,options.remove_files,options.remove_directories)
   else:
      parser.error('no options specified, doing nothing.')
   


if __name__ == "__main__":
   main()
