from django.conf import settings
import subprocess
import logging
import os,sys,traceback
import urlparse

# temporary 
import shutil
import tempfile


logger = logging.getLogger(__name__)

# - GridFTP implementation

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
         except OSError,e:
            logger.error('command failed with OSError, exception: ' + str(e))
            raise Exception('Error in pre_stage_hook, OSError raised')
         except ValueError,e:
            logger.error('command failed with ValueError, exception: ' + str(e))
            raise Exception('Error in pre_stage_hook, ValueError raised')
         except Exception,e:
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
      except OSError,e:
         logger.error('command failed with OSError, exception: ' + str(e))
         raise Exception('Error in stage_in, OSError raised')
      except ValueError,e:
         logger.error('command failed with ValueError, exception: ' + str(e))
         raise Exception('Error in stage_in, ValueError raised')
      except Exception,e:
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
      except OSError,e:
         logger.error('command failed with OSError, exception: ' + str(e))
         raise Exception('Error in stage_out, OSError raised')
      except ValueError,e:
         logger.error('command failed with ValueError, exception: ' + str(e))
         raise Exception('Error in stage_out, ValueError raised')
      except Exception,e:
         logger.error('command failed, exception traceback: \n' + traceback.format_exc() )
         raise Exception('Error in stage_out, unknown exception raised')

      if p.returncode:
         logger.error('command failed with return value: ' + str(p.returncode) + '\n  stdout: \n' + out + '\n stderr: \n' + err)
         raise Exception("Error in stage_out: %s" % str(p.returncode))


# - Local implementation

class LocalHandler:
   def pre_stage_hook(self):
      pass

   def stage_in( self, source_url, destination_directory ):
      parts = urlparse.urlparse( source_url )
      command = 'cp -p -r %s/* %s' % (parts.path, destination_directory)
      print 'transfer.stage_in: command=' + command 
      ret = os.system(command)
      if ret:
         raise Exception("Error in stage_in: %d" % ret)

   def stage_out( self, source_directory, destination_url ):
      parts = urlparse.urlparse( destination_url )
      command = 'cp -r %s/* %s' % (source_directory, parts.path)
      print 'transfer.stage_out: command=' + command
      ret = os.system(command)
      if ret:
         raise Exception("Error in stage_out: %d" % ret)

# - SCP implementation

class SCPHandler:
   def pre_stage_hook(self):
      pass

   def stage_in( self, source_url, destination_directory ):
      parts = urlparse.urlparse( source_url )
      command = 'scp -p -r %s:%s %s' % (source_url, destination_directory)
      print 'transfer.stage_in: command=' + command 
      ret = os.system(command)
      if ret:
         raise Exception("Error in stage_in: %d" % ret)

   def stage_out( self, source_directory, destination_url ):
      # ensure that source and destination each have a trailing '/'
      command = 'scp -p -r %s %s' % (source_directory, destination_url)
      print 'transfer.stage_out: command=' + command
      ret = os.system(command)
      if ret:
         raise Exception("Error in stage_out: %d" % ret)


# - Generic interface

def get_handler(url):
   handlers = {
     'gsiftp':GridFTPHandler,
     'scp'   :SCPHandler,
     'local' :LocalHandler
   }
   proto = url.split(':')[0]
   if proto in handlers.keys():
      handler_class = handlers[proto]
      handler = handler_class()
   else:
      raise Exception('Unknown transfer protocol: %s' % proto)
   return handler    

# def pre_stage_hook(url):
#     handler = get_handler(url)
#     handler.pre_stage_hook()

def stage_in( source_url, destination_directory ):
   handler = get_handler(source_url)
   logger.debug('pre-stage hook')
   handler.pre_stage_hook()
   logger.debug('stage-in')
   handler.stage_in( source_url, destination_directory )

def stage_out( source_directory, destination_url ):
   handler = get_handler(destination_url)
   handler.pre_stage_hook()
   handler.stage_out( source_directory, destination_url )
