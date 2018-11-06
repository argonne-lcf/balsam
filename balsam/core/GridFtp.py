import sys,os,subprocess,shlex,logging,time,random
from django.conf import settings
logger = logging.getLogger(__name__)

# job has completed, M,M,M,copy files from gridftp server
#logger.debug('setup X509 certificates for gridftp calls')
#os.environ['X509_USER_CERT'] = '/users/hpcusers/balsam/gridsecurity/jchilders/xrootdsrv-cert.pem'
#os.environ['X509_USER_KEY']  = '/users/hpcusers/balsam/gridsecurity/jchilders/xrootdsrv-key.pem'

RETRY_ATTEMPTS=10

def globus_url_copy(from_path,to_path):

   p = subprocess.Popen([settings.GRIDFTP_PROXY_INFO,'-exists'])
   p.wait()
   if p.returncode is not 0: # valid proxy does not exist so create one
      p = subprocess.Popen([settings.GRIDFTP_PROXY_INIT,'-verify','-debug','-bits','2048','-valid','96:00'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
      stdout = ''
      for line in p.stdout:
         logger.debug(line[0:-1])
         stdout += line
      p.wait()
      if p.returncode is not 0:
         raise Exception('grid-proxy-init failed: stdout = \n' + stdout + '\n')
      else:
         logger.debug(' grid-proxy-init stdout: \n' + stdout + '\n')


   from_path = str(from_path)
   to_path = str(to_path)
   cmd = settings.GRIDFTP_GLOBUS_URL_COPY + ' -cd  -r -nodcau ' + from_path + ' ' + to_path
   #logger.debug(' GridFtp, from_path = ' + from_path )
   #logger.debug(' GridFtp, to_path   = ' + to_path )
   
   # added random sleep to keep too many grid ftp requests from hammering the server at the same time
   time.sleep(random.randint(1,100))

   logger.debug(' transferring files: ' + cmd )
   for i in range(RETRY_ATTEMPTS):
      try:
         proc = subprocess.Popen(shlex.split(cmd),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
      except:
         logger.error(' Error while performing GridFTP transfer, retrying transfer ' + str(i+1) + ' of ' + str(RETRY_ATTEMPTS) )
         if i+1 >= RETRY_ATTEMPTS: raise
         else:
            logger.error(' waiting 10 minutes before trying transfer again ')
            time.sleep(600)
            continue
      stdout = ''
      for line in proc.stdout:
         logger.debug(line[0:-1])
         stdout += line
      p.wait()
      if p.returncode is not 0:
         logger.error(' globus-url-copy exited with non-zero return code: stdout = \n' + stdout + '\n')
         if i+1 == RETRY_ATTEMPTS:
            raise Exception('globus-url-copy: exited with non-zero return code: returncode = ' + str(p.returncode) + '\n  stdout = \n' + stdout + '\n')
         else:
            logger.error(' waiting 10 minutes before trying transfer again ')
            time.sleep(600)
            continue
      elif 'error' in stdout.lower():
         logger.error('globus-url-copy: error found in output: stdout = \n' + stdout + '\n')
         if i+1 == RETRY_ATTEMPTS:
            raise Exception('globus-url-copy: error found in output: stdout = \n' + stdout + '\n')
         else:
            logger.error(' waiting 10 minutes before trying transfer again ')
            time.sleep(600)
            continue
      
   return stdout


