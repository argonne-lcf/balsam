import logging
logger = logging.getLogger(__name__)

from balsam.service import BalsamJobStatus
from balsam.common import PikaMessageInterface, NoMessageInterface

SENDER_MAP = {
    'pika' : PikaMessageInterface.PikaMessageInterface,
    'no_message' : NoMessageInterface.NoMessageInterface
}

class BalsamStatusSender(object):
    '''Use an instance of this class to send status messages out from Balsam.
    Constructor passes the messaging protocol details on to MessageInterface'''

    def __init__(self, settings):
        sender_mode = settings['mode']
        MessageClass = SENDER_MAP[sender_mode]
        self.messageInterface = MessageClass(settings)

    def send_status(self,job,message=''):
        '''send a status message describing a job state'''
        p = self.messageInterface
        try:
            p.setup_send()
            statmsg = BalsamJobStatus.BalsamJobStatus(job, message)
            p.send_msg(statmsg.serialize())
            p.close()
        except Exception as e:
            logger.exception(
                'job(pk='+str(job.pk)+',id='+str(job.job_id)+
                '): Failed to send BalsamJobStatus message, received exception'
            )
