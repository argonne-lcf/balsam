import subprocess,logging
logger = logging.getLogger(__name__)

def write_checksum(exe,filename = None):
   try:
      p = subprocess.Popen(['openssl','md5',exe],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
   except Exception as e:
      logger.error('ERROR running MD5 checksum on executable: ' + exe + ', exception: ' + str(e))
      return None
   stdout,stderr = p.communicate()
   p = None

   if filename is not None:
      file = open(filename,'w')
      file.write(stdout)
      file.close()
      file = None
   return stdout


