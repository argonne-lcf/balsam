import os
import pathlib
    
# ---------------------------------------------
# Set default_db_path to balsam DB directory
# ---------------------------------------------
here = os.path.abspath(os.path.dirname(__file__))
here = pathlib.Path(here).parent
default_db_path = os.path.join(here , 'default_balsamdb')

#------------------------------
# BALSAM CONFIG INFO
#------------------------------
BALSAM_DELETE_OLD_WORK              = True # enable deletion of old folders in BALSAM_WORK_DIRECTORY at a period of BALSAM_DELETE_OLD_WORK_PERIOD
BALSAM_DELETE_OLD_WORK_PERIOD       = 86400 # once a day check for old work folders older than the BALSAM_DELETE_OLD_WORK_AGE
BALSAM_DELETE_OLD_WORK_AGE          = 86400 * 31 # delete work folders that are older than 31 days
BALSAM_SCHEDULER_SUBMIT_EXE         = '/usr/bin/qsub'
BALSAM_SCHEDULER_STATUS_EXE         = '/usr/bin/qstat'
BALSAM_SCHEDULER_HISTORY_EXE        = '/usr/bin/'
BALSAM_SERVICE_PERIOD               = 1 # seconds between service loop execution
BALSAM_RUNNER_CREATION_PERIOD_SEC   = 5
BALSAM_MAX_QUEUED                   = 20 # the maximum number of jobs allowed on the local queue
BALSAM_SUBMIT_JOBS                  = True # submit jobs to queue, turn off when testing
BALSAM_DEFAULT_QUEUE                = 'debug-cache-quad' # default local queue name
BALSAM_DEFAULT_PROJECT              = 'datascience' # default local project name
BALSAM_SITE                         = 'theta' # local balsam site name
BALSAM_SCHEDULER_CLASS              = 'CobaltScheduler' # local scheduler in use
BALSAM_MAX_CONCURRENT_TRANSITIONS   = 5 # maximum number of sub threads spawned by Balsam
BALSAM_MAX_CONCURRENT_RUNNERS       = 50 # maximum number of background 'mpirun' subprocesses

#------------------------------
# ARGO CONFIG INFO
#------------------------------
ARGO_SERVICE_PERIOD                 = 10 # seconds between service loop execution
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

#----------
# MESSAGING
#----------
RECEIVER_CONFIG = {
    "mode" : "no_message"
}

SENDER_CONFIG = {
    "mode" : "no_message"
}

#------------------------------
# LOGGING
#------------------------------
LOG_HANDLER_LEVEL = 'DEBUG'
LOG_FILENAME = 'balsam.log'
LOG_BACKUP_COUNT = 5 # number of files worth of history
LOG_FILE_SIZE_LIMIT = 100 * 1024 * 1024 # file size at which to move to a new log file
