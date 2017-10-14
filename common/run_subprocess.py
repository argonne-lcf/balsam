import subprocess,logging
logger = logging.getLogger(__name__)

class SubprocessFailed(Exception): pass
class SubprocessNonzeroReturnCode(Exception): pass
def run_subprocess(cmd,ignore_nonzero_return=False):
   try:
      logger.debug('run_subprocess: ' + cmd)
      p = subprocess.Popen(cmd.split(),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
      p.wait()
      stdout,stderr = p.communicate()
      if p.returncode != 0 and not ignore_nonzero_return:
         raise SubprocessNonzeroReturnCode(stdout)
      return stdout
   except Exception as e:
      logger.exception('exception received')
      raise SubprocessFailed(str(e))