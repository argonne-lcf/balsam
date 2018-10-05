from balsam import settings
import subprocess
import logging
import glob
import os,sys,traceback
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

def validate_path(path):
    path = os.path.abspath(os.path.expanduser(path))
    if glob.glob(path):
        return path
    else:
        raise ValueError(f'Nothing matches {path} in filesystem')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# - GridFTP implementation
GRIDFTP_PROTOCOL='gsiftp'
class GridFTPHandler:
   def pre_stage_hook(self):
      # check to see if proxy already exists
      p = subprocess.Popen([settings.GRIDFTP_PROXY_INFO,'-exists'])
      p.wait()
      if p.returncode is not 0: # valid proxy does not exist so create one
         command = str(settings.GRIDFTP_PROXY_INIT) + ' -verify -debug -bits 2048 -valid 96:00'
         logger.debug('command=' + command)
         try:
            p = subprocess.Popen(command.split(' '),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            out,err = p.communicate()
         except OSError as e:
            logger.error('command failed with OSError, exception: ' + str(e))
            raise Exception('Error in pre_stage_hook, OSError raised')
         except ValueError as e:
            logger.error('command failed with ValueError, exception: ' + str(e))
            raise Exception('Error in pre_stage_hook, ValueError raised')
         except Exception as e:
            logger.error('command failed, exception traceback: \n' + traceback.format_exc() )
            raise Exception('Error in stage_in, unknown exception raised')

         if p.returncode:
            logger.error('command failed with return value: ' + str(p.returncode) + '\n  stdout: \n' + out + '\n stderr: \n' + err)
            raise Exception('Error in pre_stage_hook, return value = ' + str(p.returncode) )
         else:
            logger.debug('gridftp initialization completed, stdout:\n' + out + '\n stderr:\n' + err )

   def stage_in( self, source_url, destination_directory ):
      # ensure that source and destination each have a trailing '/'
      if source_url[-1] != '/':
         source_url += '/'
      if destination_directory[-1] != '/':
         destination_directory += '/'
      command = str(settings.GRIDFTP_GLOBUS_URL_COPY) + ' -dbg -nodcau -r %s %s' % (source_url, destination_directory)
      logger.debug('command=' + command )
      try:
         p = subprocess.Popen(command.split(' '),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
         out,err = p.communicate()
      except OSError as e:
         logger.error('command failed with OSError, exception: ' + str(e))
         raise Exception('Error in stage_in, OSError raised')
      except ValueError as e:
         logger.error('command failed with ValueError, exception: ' + str(e))
         raise Exception('Error in stage_in, ValueError raised')
      except Exception as e:
         logger.error('command failed, exception traceback: \n' + traceback.format_exc() )
         raise Exception('Error in stage_in, unknown exception raised')
      if p.returncode:
         logger.error('command failed with return value: ' + str(p.returncode) + '\n  stdout: \n' + out + '\n stderr: \n' + err)
         raise Exception("Error in stage_in: %s" % str(p.returncode))

   def stage_out( self, source_directory, destination_url ):
      # ensure that source and destination each have a trailing '/'
      if source_directory[-1] != '/':
         source_directory += '/'
      if destination_url[-1] != '/':
         destination_url += '/'
      command = str(settings.GRIDFTP_GLOBUS_URL_COPY) + ' -dbg -nodcau -cd -r %s %s' % (source_directory, destination_url)
      logger.debug('command=' + command)
      try:
         p = subprocess.Popen(command.split(' '),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
         out,err = p.communicate()
      except OSError as e:
         logger.error('command failed with OSError, exception: ' + str(e))
         raise Exception('Error in stage_out, OSError raised')
      except ValueError as e:
         logger.error('command failed with ValueError, exception: ' + str(e))
         raise Exception('Error in stage_out, ValueError raised')
      except Exception as e:
         logger.error('command failed, exception traceback: \n' + traceback.format_exc() )
         raise Exception('Error in stage_out, unknown exception raised')

      if p.returncode:
         logger.error('command failed with return value: ' + str(p.returncode) + '\n  stdout: \n' + out + '\n stderr: \n' + err)
         raise Exception("Error in stage_out: %s" % str(p.returncode))


# - Local implementation
LOCAL_PROTOCOL='local'
class LocalHandler:
   def pre_stage_hook(self):
      pass

   def stage_in(self, source_url, destination_directory):
      if source_url.strip().startswith('local:'):
          source = ''.join(source_url.split(':')[1:])
      else:
          source = source_url
      source = validate_path(source)
      dest = validate_path(destination_directory)
      assert os.path.isdir(dest), f'{dest} is not a valid destination directory'

      command = f'cp -p -r {source} {destination_directory}'
      logger.debug('transfer.stage_in: command=' + command )
      p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT, shell=True)
      stdout,stderr = p.communicate()
      if p.returncode != 0:
          raise Exception(f"Stage in process returned {p.returncode}: {stdout}")

   def stage_out(self, source_directory, destination_url):
      if destination_url.strip().startswith('local:'):
          dest = ''.join(destination_url.split(':')[1:])
      else:
          dest = destination_url
      source = validate_path(source_directory)
      dest = validate_path(dest)
      assert os.path.isdir(dest), f'{dest} is not a valid destination directory'

      command = f'cp -r {source} {dest}'
      logger.debug('transfer.stage_out: command=' + command )
      p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT, 
              shell=True)
      stdout,stderr = p.communicate()
      if p.returncode != 0:
         raise Exception("Error in stage_out: %d output:\n" % (p.returncode,stdout))

# - SCP implementation
SCP_PROTOCOL='scp'
class SCPHandler:
   def pre_stage_hook(self):
      pass

   def stage_in( self, source_url, destination_directory ):
      parts = urlparse.urlparse( source_url )
      command = 'scp -p -r %s:%s %s' % (source_url, destination_directory)
      logger.debug('transfer.stage_in: command=' + command )
      ret = os.system(command)
      if ret:
         raise Exception("Error in stage_in: %d" % ret)

   def stage_out( self, source_directory, destination_url ):
      # ensure that source and destination each have a trailing '/'
      command = 'scp -p -r %s %s' % (source_directory, destination_url)
      logger.debug('transfer.stage_out: command=' + command)
      ret = os.system(command)
      if ret:
         raise Exception("Error in stage_out: %d" % ret)


# - Generic interface

def get_handler(url):
    handlers = {
        GRIDFTP_PROTOCOL:GridFTPHandler,
        SCP_PROTOCOL    :SCPHandler,
        LOCAL_PROTOCOL  :LocalHandler
    }
    proto = url.split(':')[0]
    if proto in handlers.keys():
        handler_class = handlers[proto]
        handler = handler_class()
    else:
        url = validate_path(url)
        handler = LocalHandler()
    return handler

def stage_in(source_url, destination_directory):
    handler = get_handler(source_url)
    handler.pre_stage_hook()
    handler.stage_in(source_url, destination_directory)

def stage_out(source_directory, destination_url):
    handler = get_handler(destination_url)
    handler.pre_stage_hook()
    handler.stage_out(source_directory, destination_url)
