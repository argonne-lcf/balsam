import logging
logger = logging.getLogger('console')

# redirect uncaught exceptions to the logger
def log_uncaught_exceptions(exctype, value, tb,logger=logger):
   logger.error('Uncaught Exception')
   logger.error('Type: ' + str(exctype))
   logger.error('Value:' + str(value))
   logger.error('Traceback:',exc_info=(exctype, value, tb))
# assign this function to the system exception hook