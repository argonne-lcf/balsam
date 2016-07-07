import os,sys,logging
logger = logging.getLogger(__name__)


def get_sanitized_exe(allowed_path,exe):
   if exe is None:
      return None
   local_full_path_to_exe = os.path.join(allowed_path,os.path.basename(exe))
   if os.path.exists(local_full_path_to_exe):
      return local_full_path_to_exe
   else:
      raise Exception(' input executable, ' + str(exe) + ', is not in the allowed path: ' + str(allowed_path))
      
def get_sanitized_arguments(arguments):
   if arguments is None: return None
   if arguments.find(';') >= 0:
      raise Exception(' input arguments are not sanitary: ' + str(arguments))
   return arguments


def get_sanitized_command(allowed_exe_path,exe,arguments):
   if exe is None: return None
   exe = get_sanitized_exe(allowed_exe_path,exe)

   args = get_sanitized_arguments(arguments)
   if args is None:
      return None

   return exe + ' ' + arguments
   
   

