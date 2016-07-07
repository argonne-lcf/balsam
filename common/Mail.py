import sys,subprocess,logging
logger = logging.getLogger(__name__)

MAIL = 'mail'

def send_mail(sender,receiver,subject,body):
   
   p = subprocess.Popen([MAIL,'-s',str(subject),receiver],stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE)
   stdout,stdin = p.communicate(input=str(body))


#send_mail('','jchilders@anl.gov','test subject','test body')


