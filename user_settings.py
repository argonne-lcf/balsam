import os,sys,logging
import socket
from django.core.management import call_command
logger = logging.getLogger('console')
    
try:
   INSTALL_PATH = os.environ['ARGOBALSAM_INSTALL_PATH']
except KeyError as e:
   logger.error('Environment not setup: ' + str(e))
   raise


#------------------------------
# DATABASE CONFIG INFO
#------------------------------
CONCURRENCY_ENABLED = True
USING_DB_LOGIN                      = False
DBUSER                              = ''
DBPASS                              = ''
if USING_DB_LOGIN:
   DBUSER                           = os.environ['ARGOBALSAM_DBUSER']
   DBPASS                           = os.environ['ARGOBALSAM_DBPASS']

# Database
# https://docs.djangoproject.com/en/1.9/ref/settings/#databases
default_db = {}
default_db['ENGINE'] = 'django.db.backends.sqlite3'
default_db['NAME'] = os.path.join(INSTALL_PATH,'db.sqlite3')
default_db['OPTIONS'] = {'timeout' : 500000.0}
if USING_DB_LOGIN:
   default_db['USER'] = DBUSER
   default_db['PASSWORD'] = DBPASS


testing = os.environ.get('BALSAM_TEST', '')
if testing:
    INSTALL_PATH = os.environ['BALSAM_TEST_DIRECTORY']
    db_test = default_db.copy()
    db_test['NAME'] = os.path.join(INSTALL_PATH, 'test_db.sqlite3')
    DATABASES = {'default':db_test}
else:
    DATABASES = {'default':default_db}

DATA_PATH = os.path.join(INSTALL_PATH,'data')
ALLOWED_EXE_PATH = os.path.join(INSTALL_PATH,'exe')

#------------------------------
# BALSAM CONFIG INFO
#------------------------------
BALSAM_WORK_DIRECTORY               = os.path.join(DATA_PATH,'balsamjobs') # where to store local job data used for submission
BALSAM_DELETE_OLD_WORK              = True # enable deletion of old folders in BALSAM_WORK_DIRECTORY at a period of BALSAM_DELETE_OLD_WORK_PERIOD
BALSAM_DELETE_OLD_WORK_PERIOD       = 86400 # once a day check for old work folders older than the BALSAM_DELETE_OLD_WORK_AGE
BALSAM_DELETE_OLD_WORK_AGE          = 86400 * 31 # delete work folders that are older than 31 days
BALSAM_SCHEDULER_SUBMIT_EXE         = '/usr/bin/qsub'
BALSAM_SCHEDULER_STATUS_EXE         = '/usr/bin/qstat'
BALSAM_SCHEDULER_HISTORY_EXE        = '/usr/bin/'
BALSAM_SERVICE_PERIOD               = 1 # seconds between service loop execution
BALSAM_MAX_QUEUED                   = 20 # the maximum number of jobs allowed on the local queue
BALSAM_SUBMIT_JOBS                  = True # submit jobs to queue, turn off when testing
BALSAM_DEFAULT_QUEUE                = 'debug-cache-quad' # default local queue name
BALSAM_DEFAULT_PROJECT              = 'datascience' # default local project name
BALSAM_ALLOWED_EXECUTABLE_DIRECTORY = ALLOWED_EXE_PATH # path to allowed executables
BALSAM_SITE                         = 'theta' # local balsam site name
BALSAM_SCHEDULER_CLASS              = 'CobaltScheduler' # local scheduler in use
BALSAM_MAX_CONCURRENT_TRANSITIONS   = 5 # maximum number of sub threads spawned by Balsam
BALSAM_MAX_CONCURRENT_RUNNERS       = 50 # maximum number of background 'mpirun' subprocesses

#------------------------------
# ARGO CONFIG INFO
#------------------------------
ARGO_SERVICE_PERIOD                 = 10 # seconds between service loop execution
ARGO_WORK_DIRECTORY                 = os.path.join(DATA_PATH,'argojobs')
ARGO_DELETE_OLD_WORK                = True # enable deletion of old folders in BALSAM_WORK_DIRECTORY at a period of ARGO_DELETE_OLD_WORK_PERIOD
ARGO_DELETE_OLD_WORK_PERIOD         = 86400 # in seconds,  once a day check for old work folders older than
ARGO_DELETE_OLD_WORK_AGE            = 86400 * 31 # in seconds,  delete work folders that are older than 31 days
ARGO_MAX_CONCURRENT_TRANSITIONS     = 5 # maximum number of sub threads spawned by ARGO


#------------------------------
# GRID FTP SERVER INFO
#------------------------------

GRIDFTP_BIN                         = '/soft/data-transfer/globus/bin'
GRIDFTP_GLOBUS_URL_COPY             = os.path.join(GRIDFTP_BIN,'globus-url-copy')
GRIDFTP_PROXY_INFO                  = os.path.join(GRIDFTP_BIN,'grid-proxy-info')
GRIDFTP_PROXY_INIT                  = os.path.join(GRIDFTP_BIN,'grid-proxy-init')
GRIDFTP_PROTOCOL                    = 'gsiftp://'
GRIDFTP_SERVER                      = ''

#------------------------------
# MESSAGING CONFIG
# Specify everything in a SENDER_CONFIG for BalsamStatusSender
# And in RECEIVER_CONFIG for BalsamJobReceiver
#------------------------------
#SENDER_CONFIG = {
#    "mode"                          : "pika",
#    "username"                      : '',
#    "password"                      : '',
#    "host"                          : '',
#    "port"                          : '',
#    "virtual_host"                  : '',
#    "socket_timeout"                : '',
#    "exchange_name"                 : '',
#    "exchange_type"                 : '',
#    "exchange_durable"              : '',
#     "exchange_auto_delete"         : '',
#    "queue_name"                    : '',
#    "queue_is_durable"              : '',
#    "queue_is_exclusive"            : '',
#    "queue_is_auto_delete"          : '',
#    "default_routing_key"           : '',
#     "ssl_cert"                     : '',
#    "ssl_key"                       : '',
#     "ssl_ca_cer"                   : ''
#}
#RECEIVER_CONFIG = {
#    "mode"                          : "pika",
#    "username"                      : '',
#    "password"                      : '',
#    "host"                          : '',
#    "port"                          : '',
#    "virtual_host"                  : '',
#    "socket_timeout"                : '',
#    "exchange_name"                 : '',
#    "exchange_type"                 : '',
#    "exchange_durable"              : '',
#     "exchange_auto_delete"         : '',
#    "queue_name"                    : '',
#    "queue_is_durable"              : '',
#    "queue_is_exclusive"            : '',
#    "queue_is_auto_delete"          : '',
#    "default_routing_key"           : '',
#     "ssl_cert"                     : '',
#    "ssl_key"                       : '',
#     "ssl_ca_cer"                   : ''
#}

RECEIVER_CONFIG = {
    "mode" : "no_message"
}

SENDER_CONFIG = {
    "mode" : "no_message"
}


#------------------------------
# logging settings
#------------------------------
LOGGING_DIRECTORY = os.path.join(INSTALL_PATH, 'log') # where to store log files
LOG_HANDLER_LEVEL = 'DEBUG'
LOG_BACKUP_COUNT = 5 # number of files worth of history
LOG_FILE_SIZE_LIMIT = 100 * 1024 * 1024 # file size at which to move to a new log file

if 'argoservice' in ' '.join(sys.argv):
    HANDLER_FILE = 'argo.log'
elif 'service' in ' '.join(sys.argv):
    HANDLER_FILE = 'balsam.log'
elif 'launcher' in ' '.join(sys.argv):
    HANDLER_FILE = 'launcher.log'
else:
    HANDLER_FILE = 'misc.log'
HANDLER_FILE = os.path.join(LOGGING_DIRECTORY, HANDLER_FILE)

LOGGING = {
   'version': 1,
   'disable_existing_loggers': False,
   'formatters': {
      'standard': {
      'format' : '%(asctime)s|%(process)d|%(levelname)8s|%(name)s:%(lineno)s] %(message)s',
      'datefmt' : "%d-%b-%Y %H:%M:%S"
      },
   },
   'handlers': {
      'console': {
         'class':'logging.StreamHandler',
         'formatter': 'standard',
          'level' : 'DEBUG'
      },
      'default': {
         'level':LOG_HANDLER_LEVEL,
         'class':'logging.handlers.RotatingFileHandler',
         'filename': HANDLER_FILE,
         'maxBytes': LOG_FILE_SIZE_LIMIT,
         'backupCount': LOG_BACKUP_COUNT,
         'formatter': 'standard',
      }
   },
   'loggers': {
      'django':{
         'handlers': ['default'],
         'level': 'DEBUG',
         'propagate': True,
      },
      'argo': {
         'handlers': ['default'],
         'level': 'DEBUG',
      },
      'common': {
         'handlers': ['default'],
         'level': 'DEBUG',
      },
      'console': {
         'handlers': ['console'],
         'level': 'INFO',
      },
      'balsam': {
         'handlers': ['default'],
         'level': 'DEBUG',
      },
      'balsamlauncher': {
         'handlers': ['default'],
         'level': 'DEBUG',
      },
      'django.db.backends': {
         'handlers':['default'],
         'level': 'WARNING',
      },
      'pika.adapters.base_connection': {
         'handlers':['default'],
         'level':'WARNING',
      },
   }
}
if 'argoservice' in ' '.join(sys.argv):
    logger = logging.getLogger('argo')
elif 'service' in sys.argv:
    logger = logging.getLogger('balsam')
elif 'launcher' in ' '.join(sys.argv):
    logger = logging.getLogger('balsamlauncher')
else:
    logger = logging.getLogger('console')

def log_uncaught_exceptions(exctype, value, tb,logger=logger):
   logger.error(f"Uncaught Exception {exctype}: {value}",exc_info=(exctype,value,tb))
   logger = logging.getLogger('console')
   logger.error(f"Uncaught Exception {exctype}: {value}",exc_info=(exctype,value,tb))

sys.excepthook = log_uncaught_exceptions

#------------------------------
# Sanity Checks
#------------------------------

# ensure that requisite paths exist
for d in [
      INSTALL_PATH,
      DATA_PATH,
      ALLOWED_EXE_PATH,
      LOGGING_DIRECTORY,
      BALSAM_WORK_DIRECTORY,
      BALSAM_ALLOWED_EXECUTABLE_DIRECTORY,
      ARGO_WORK_DIRECTORY,
      #GRIDFTP_GLOBUS_URL_COPY,
      #GRIDFTP_PROXY_INFO,
      #GRIDFTP_PROXY_INIT,
      #RABBITMQ_SSL_CERT,
      #RABBITMQ_SSL_KEY,
      #RABBITMQ_SSL_CA_CERTS,
   ]:
   if not os.path.exists(d):
      os.makedirs(d)
      logger.info(f'Created directory {d}')
