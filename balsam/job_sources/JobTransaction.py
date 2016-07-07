import xml.etree.ElementTree as ET
import sys,os,ssl
import pika,time

import logging
logger = logging.getLogger(__name__)
logging.getLogger('pika').setLevel(logging.WARNING)

EXCHANGE_NAME        = 'hpc'
EXCHANGE_TYPE        = 'topic'
EXCHANGE_DURABLE     = True
EXCHANGE_AUTO_DELETE = False
SOCKET_TIMEOUT       = 120
VIRTUAL_HOST         = '/'
SERVER_PORT          = 40002
SERVER_NAME          = 'atlasgridftp02.hep.anl.gov'
SERVER_USER          = 'guest'
SERVER_PASS          = 'guest'

TRUE                 = 'True'
FALSE                = 'False'

class XmlTags:
   job            = 'job'
   job_id         = 'job_id'
   user           = 'user'
   task           = 'task'
   program        = 'program'
   exe            = 'exe'
   args           = 'args'
   preprocess     = 'preprocess'
   postprocess    = 'postprocess'
   transfer_in    = 'transfer_in'
   transfer_out   = 'transfer_out'
   input_files    = 'input_files'
   output_files   = 'output_files'
   nodes          = 'nodes'
   scheduler_args = 'scheduler_args'
   cpus_per_node  = 'cpus_per_node'
   task_duration_minutes  = 'task_duration_minutes'
   nevents        = 'nevents'

class JobDescription:
   ''' Output/Input the job XML file '''

   
   def __init__(self,
                job_id           = None,
                num_evts         = None,
                exe              = None,
                exe_args         = None,
                nodes            = None,
                scheduler_args   = None,
                cpus_per_node    = None,
                task_duration_minutes = None,
                user             = None,
                preprocess       = None,
                preprocess_args  = None,
                postprocess      = None,
                postprocess_args = None,
                transfer_in      = FALSE,
                input_files      = None,
                transfer_out     = FALSE,
                output_files     = None,
               ):
      self.job_id             = job_id             # unique job identification number
      self.num_evts           = num_evts           # number of events to generate
      self.exe                = exe                # name of executable
      self.exe_args           = exe_args           # arguments for executable
      self.nodes              = nodes              # number of nodes to use for this job
      self.scheduler_args     = scheduler_args     # arguments passed to scheduler at submit time
      self.cpus_per_node      = cpus_per_node      # number of cpus per node to use for this job
      self.task_duration_minutes = task_duration_minutes # time needed for this job, used for queue timing
      self.user               = user               # name of user submitting, used for authentication
      self.preprocess         = preprocess         # executable name to run before exe
      self.preprocess_args    = preprocess_args    # args for this executable
      self.postprocess        = postprocess        # executable name to run after exe
      self.postprocess_args   = postprocess_args   # args for this executable
      self.transfer_in        = transfer_in        # set to TRUE/FALSE if files need to be transferred to HPC before job is run
      self.transfer_out       = transfer_out       # set to TRUE/FALSE if files need to be transferred fram HPC after job is complete
      self.input_files        = input_files        # name of the input files the job will need
      self.output_files       = output_files       # name of the output files produced by the job that are needed
      self.job_xml            = None   # internal variable that keeps the xml ElementTree
      self.job_xml_pretty     = None   # interval variable that keeps the pretty xml ElementTree

   def __str__(self):
      out  = '\n'
      out += '------------------------------------------------------------------\n'
      out += 'Job Description for Job ID: ' + str(self.job_id) + ' for user: ' + str(self.user) + '\n'
      out += '   exe + args:       ' + str(self.exe)
      if self.exe_args: out += ' ' + str(self.exe_args)
      out += '\n'
      out += '     nodes: ' + str(self.nodes) + '; cpus_per_node: ' + str(self.cpus_per_node) + '\n'
      out += '     scheduler_args: ' + str(self.scheduler_args) + '\n'
      out += '     task_duration_minutes: ' + str(self.task_duration_minutes) + '; num_evts: ' + str(self.num_evts) + '\n'
      out += '   preprocess + args:  '
      if self.preprocess: out += str(self.preprocess)
      if self.preprocess_args: out += ' ' + str(self.preprocess_args)
      out += '\n'
      out += '   postprocess + args: '
      if self.postprocess: out += str(self.postprocess)
      if self.postprocess_args: out += ' ' + str(self.postprocess_args)
      out += '\n'
      out += '   transfer_in: ' + str(self.transfer_in) + '\n'
      out += '   transfer_out: ' + str(self.transfer_out) + '\n'
      out += '   input_files: '
      if self.input_files: out += str(self.input_files)
      out += '\n'
      out += '   output_files: '
      if self.output_files: out += str(self.output_files)
      out += '\n'
      out += '-------------------------------------------------------------------\n'
      return out

class BalsamMessage:
   NO_MESSAGE        = 0x0
   SUCCEEDED         = 0x1 << 0
   SUBMIT_DISABLED   = 0x1 << 1
   FAILED            = 0x1 << 2
   INVALID_EXE       = 0x1 << 3


   message_list = [
                   NO_MESSAGE,
                   SUCCEEDED,
                   SUBMIT_DISABLED,
                   FAILED,
                   INVALID_EXE,
                  ]
   message_text = {
                   NO_MESSAGE:'no message',
                   SUCCEEDED:'succeeded',
                   SUBMIT_DISABLED:'submission disabled',
                   FAILED:'failed',
                   INVALID_EXE:'invalid executable',
                  }


   def __init__(self):
      self.message = BalsamMessage.NO_MESSAGE

   def __str__(self):
      out = []
      for message in BalsamMessage.message_list:
         if message & self.message:
            out.append(BalsamMessage.message_text[message])
      return str(out)

   def contains(self,flag):
      if flag in BalsamMessage.message_list:
         if self.message & flag:
            return True
      else:
         logger.warning('Invalid Flag passed')
      return False



''' Class for handling the message queue interactions to/from the RabbitMQ Server
    JobMsgHandler is for the job side of things
    BalsamMsgHandler is for the Balsam side of things '''
class JobMsgHandler:
   ''' handle communication with Balsam on the Job-side '''
   
   def __init__(self,
                username                  = SERVER_USER,
                password                  = SERVER_PASS,
                host                      = SERVER_NAME,
                port                      = SERVER_PORT,
                virtual_host              = VIRTUAL_HOST,
                socket_timeout            = SOCKET_TIMEOUT,
                exchange_name             = EXCHANGE_NAME,
                exchange_type             = EXCHANGE_TYPE,
                exchange_durable          = EXCHANGE_DURABLE,
                exchange_auto_delete      = EXCHANGE_AUTO_DELETE,
                job_queue_is_durable      = True,
                job_queue_is_exclusive    = True,
                job_queue_is_auto_delete  = True,
                job_id                    = 0,
               ):
      self.username                 = username
      self.password                 = password
      self.host                     = host
      self.port                     = port
      self.virtual_host             = virtual_host
      self.socket_timeout           = socket_timeout
      self.exchange_name            = exchange_name
      self.exchange_type            = exchange_type
      self.exchange_durable         = exchange_durable
      self.exchange_auto_delete     = exchange_auto_delete
      self.job_queue_is_durable     = job_queue_is_durable
      self.job_queue_is_exclusive   = job_queue_is_exclusive
      self.job_queue_is_auto_delete = job_queue_is_auto_delete
      self.job_id                   = job_id
      
      # need to set credentials to login to the message server
      #self.credentials = pika.PlainCredentials(username,password)
      self.credentials = pika.credentials.ExternalCredentials()
      ssl_options_dict = {
                          "certfile":  "/users/hpcusers/balsam/gridsecurity/jchilders/xrootdsrv-cert.pem",
                          "keyfile":   "/users/hpcusers/balsam/gridsecurity/jchilders/xrootdsrv-key.pem",
                          "ca_certs":  "/users/hpcusers/balsam/gridsecurity/jchilders/cacerts.pem",
                          "cert_reqs": ssl.CERT_REQUIRED,
                         }
      logger.debug(str(ssl_options_dict))
      
      # setup our connection parameters
      self.parameters = pika.ConnectionParameters(
                                                  host               = self.host,
                                                  port               = self.port,
                                                  virtual_host       = self.virtual_host,
                                                  credentials        = self.credentials,
                                                  socket_timeout     = self.socket_timeout,
                                                  ssl                = True,
                                                  ssl_options        = ssl_options_dict,
                                                 )

      # open the connection and grab the channel
      self.connection = pika.BlockingConnection(self.parameters)
      self.channel   = self.connection.channel()
      
      # make sure exchange exists (doesn't do anything if already created)
      self.channel.exchange_declare(
                                    exchange       = self.exchange_name, 
                                    exchange_type  = self.exchange_type,
                                    durable        = self.exchange_durable,
                                    auto_delete    = self.exchange_auto_delete,
                                   )

      # declare an random queue which this job will use to receive messages
      # durable = survive reboots of the broker
      # exclusive = only current connection can access this queue
      # auto_delete = queue will be deleted after connection is closed
      self.channel.queue_declare(
                                 queue       = str(self.job_id),
                                 durable     = self.job_queue_is_durable,
                                 exclusive   = self.job_queue_is_exclusive,
                                 auto_delete = self.job_queue_is_auto_delete
                                )
      
      # now bind this queue to the exchange, using a routing key of the job ID
      # therefore, any message submitted to the echange with the job ID as the 
      # routing key will appear on this queue
      self.channel.queue_bind(exchange=self.exchange_name,
                              queue=str(self.job_id),
                              routing_key=str(self.job_id)
                             )
      
   def send_submit_job_msg(self,
                           machine_name,
                           operation,
                           job_description,
                           priority = 0, # make message persistent
                           delivery_mode = 2, # default
                          ):
      logger.debug('sending submit job message')
      timestamp = time.time()
      # create a header
      headers = {
                 'hpc':        machine_name,
                 'taskID':     self.job_id,
                 'operation':  operation,
                 'created':    int(timestamp),
                }

      # create the message properties
      properties = pika.BasicProperties(
                                        delivery_mode = delivery_mode,
                                        priority      = priority,
                                        timestamp     = timestamp,
                                        headers       = headers,
                                       )
      message_body = JobXmlParser.get_job_xml_as_pretty_string(job_description)
      
      if message_body is None:
         logger.fatal('JobMsgHandler.send_submit_job_msg: Failed to parse job description')
         sys.exit(-1)

      logger.debug("sending message body:\n" +  str(message_body))
      logger.debug('sending message to exchange: ' + self.exchange_name)
      logger.debug('sending message with routing key: ' + machine_name)
      
      self.channel.basic_publish(
                                 exchange         = self.exchange_name,
                                 routing_key      = machine_name,
                                 body             = message_body,
                                 properties       = properties,
                                )

   def recv_job_status_msg(self):
      logger.debug(' checking for job status message')
      msg = BalsamMessage()
      # retrieve one message
      method, properties, body = self.channel.basic_get(queue=str(self.job_id))
      if method:
         # acknowledge receipt of the message
         self.channel.basic_ack(method.delivery_tag)
         msg.message = int(body)
         logger.debug('JobMsgHandler.recv_job_status_msg: received message: ' + body + ' = ' + str(msg) )
      return msg

class BalsamMsgHandler:
   ''' handle communication with Job on Balsam-side '''
   
   def __init__(self,
                username                        = SERVER_USER,
                password                        = SERVER_PASS,
                host                            = SERVER_NAME,
                port                            = SERVER_PORT,
                virtual_host                    = VIRTUAL_HOST,
                socket_timeout                  = SOCKET_TIMEOUT,
                exchange_name                   = EXCHANGE_NAME,
                exchange_type                   = EXCHANGE_TYPE,
                exchange_durable                = EXCHANGE_DURABLE,
                exchange_auto_delete            = EXCHANGE_AUTO_DELETE,
                machine_name                    = 'localhost',
                machine_queue_is_durable        = True,
                machine_queue_is_exclusive      = False,
                machine_queue_is_auto_delete    = False,
               ):
      self.username                       = username
      self.password                       = password
      self.host                           = host
      self.port                           = port
      self.virtual_host                   = virtual_host
      self.socket_timeout                 = socket_timeout
      self.exchange_name                  = exchange_name
      self.exchange_type                  = exchange_type
      self.exchange_durable               = exchange_durable
      self.exchange_auto_delete           = exchange_auto_delete
      self.machine_name                   = machine_name
      self.machine_queue_is_durable       = machine_queue_is_durable
      self.machine_queue_is_exclusive     = machine_queue_is_exclusive
      self.machine_queue_is_auto_delete   = machine_queue_is_auto_delete

     
      # need to set credentials to login to the message server
      #self.credentials = pika.PlainCredentials(username,password)
      self.credentials = pika.credentials.ExternalCredentials()
      ssl_options_dict = {
                          "certfile":  "/users/hpcusers/balsam/gridsecurity/jchilders/xrootdsrv-cert.pem",
                          "keyfile":   "/users/hpcusers/balsam/gridsecurity/jchilders/xrootdsrv-key.pem",
                          "ca_certs":  "/users/hpcusers/balsam/gridsecurity/jchilders/cacerts.pem",
                          "cert_reqs": ssl.CERT_REQUIRED,
                         }
      logger.debug(str(ssl_options_dict))
      
      # setup our connection parameters
      self.parameters = pika.ConnectionParameters(
         host=self.host,
         port=self.port,
         virtual_host=self.virtual_host,
         credentials=self.credentials,
         socket_timeout=self.socket_timeout,
         ssl=True,
         ssl_options=ssl_options_dict)
      
      # open the connection and grab the channel
      self.connection = pika.BlockingConnection(self.parameters)
      self.channel = self.connection.channel()
      
      # make sure exchange exists (doesn't do anything if already created)
      self.channel.exchange_declare(
                                    exchange          = self.exchange_name, 
                                    exchange_type     = self.exchange_type,
                                    durable           = self.exchange_durable,
                                    auto_delete       = self.exchange_auto_delete
                                   )

      # declare a queue which balsam will use to receive job submission messages
      # durable = survive reboots of the broker
      # exclusive = only current connection can access this queue
      # auto_delete = queue will be deleted after connection is closed
      self.channel.queue_declare(
                                 queue         = self.machine_name,
                                 durable       = self.machine_queue_is_durable,
                                 exclusive     = self.machine_queue_is_exclusive,
                                 auto_delete   = self.machine_queue_is_auto_delete
                                )
      
      # now bind this queue to the exchange, using a routing key of the job ID
      # therefore, any message submitted to the echange with the job ID as the 
      # routing key will appear on this queue
      self.channel.queue_bind(
                              exchange    = self.exchange_name,
                              queue       = self.machine_name,
                              routing_key = self.machine_name
                             )
   
   def send_job_failed(self,job_id,message=None):
      msg = BalsamMessage.FAILED
      if message is not None:
         msg = (BalsamMessage.FAILED | message)
      self.send_job_status_msg(operation = '',job_id=job_id,message=msg)

   def send_job_finished(self,job_id,message=None):
      msg = BalsamMessage.SUCCEEDED
      if message is not None:
         msg = (BalsamMessage.SUCCEEDED | message)
      self.send_job_status_msg(operation='',job_id=job_id,message=msg)

   def send_job_status_msg(self,
                           operation,
                           job_id,
                           message = '',
                           priority = 0, # make message persistent
                           delivery_mode = 2, # default
                          ):
      logger.debug('sending job status message')
      timestamp = time.time()
      # create a header
      headers = {
                 'hpc':        self.machine_name,
                 'taskID':     job_id,
                 'operation':  operation,
                 'created':    int(timestamp),
                }

      # create the message properties
      properties = pika.BasicProperties(
                                        delivery_mode = delivery_mode,
                                        priority      = priority,
                                        timestamp     = timestamp,
                                        headers       = headers,
                                       )

      self.channel.basic_publish(
                                 exchange         = self.exchange_name,
                                 routing_key      = str(job_id),
                                 body             = str(message),
                                 properties       = properties,
                                )

   def recv_new_job(self):
      logger.debug(' checking for new job message')
      # retrieve one message
      method, properties, body = self.channel.basic_get(queue=self.machine_name)
      if method is not None:
         # acknowledge receipt of the message
         self.channel.basic_ack(method.delivery_tag)
         logger.debug('BalsamMsgHandler.recv_new_job: received message')
         job_description = JobXmlParser.parse_job_from_xml_string(body)
         logger.debug(str(job_description))
         return job_description
      else:
         logger.debug('No new job message received')
         raise NoMoreJobs('No jobs available') 
      return None
   
   def get_jobs_to_submit(self):
      jobs = []
      for i in range(10):
         try:
            new_job = self.recv_new_job()
         except NoMoreJobs,e:
            logger.debug('Done retrieving jobs')
            break
         jobs.append(new_job)
      logger.debug('retrieved ' + str(len(jobs)) + ' jobs to process.')
      return jobs

   def get_jobs_to_estimate(self):
      return []

   def send_job_estimate(self,url,estimate):
      pass





''' Class of functions to parse JobDescription into XML format for Message Q,
    or to parse XML from Message Q into a JobDescription '''

class JobXmlParser:
  
   @staticmethod
   def parse_job_from_xml_file(filename):
      logger.debug('parse_job_from_xml_file: Parsing XML from file: ' + filename)
      tree = ET.parse(filename)
      job_element = tree.getroot()
      return JobXmlParser.extract_job_from_xml_tree(job_element)

   @staticmethod
   def parse_job_from_xml_string(xml_string):
      logger.debug('parse_job_from_xml_string: Parsing XML: \n' + xml_string)
      job_element = ET.fromstring(xml_string)
      return JobXmlParser.extract_job_from_xml_tree(job_element)



   @staticmethod
   def extract_job_from_xml_tree(job_element):
      job = JobDescription()
      job.job_xml = job_element
      # parse job_id
      job.job_id = JobXmlParser.get_child_tag_text_int(job_element,XmlTags.job_id)
      
      # parse username
      job.user = JobXmlParser.get_child_tag_text(job_element,XmlTags.user)
      
      # parse trasnfer in
      job.transfer_in = JobXmlParser.get_child_tag_text(job_element,XmlTags.transfer_in)

      # parse transfer out
      job.transfer_out = JobXmlParser.get_child_tag_text(job_element,XmlTags.transfer_out)

      # parse the task details
      task = JobXmlParser.get_child_element(job_element,XmlTags.task)
      if task:
         # parse the number of events for this task
         job.num_evts = JobXmlParser.get_child_tag_text_int(task,XmlTags.nevents)

         # parse the cpus per node for this task
         job.nodes = JobXmlParser.get_child_tag_text_int(task,XmlTags.nodes)

         # parse the shceduler args for this task
         job.scheduler_args = JobXmlParser.get_child_tag_text(task,XmlTags.scheduler_args)
         
         # parse the cpus per node for this task
         job.cpus_per_node = JobXmlParser.get_child_tag_text_int(task,XmlTags.cpus_per_node)

         # parse the task duration in minutes for this task
         job.task_duration_minutes = JobXmlParser.get_child_tag_text_int(task,XmlTags.task_duration_minutes)

         # parse the program exe for this task
         program = JobXmlParser.get_child_element(task,XmlTags.program)
         if program:
            # parse the exe/args
            job.exe = JobXmlParser.get_child_tag_text(program,XmlTags.exe)
            job.exe_args = JobXmlParser.get_child_tag_text(program,XmlTags.args)

         # parse the pre/post process exe/args
         preprocess = JobXmlParser.get_child_element(task,XmlTags.preprocess)
         if preprocess:
            job.preprocess = JobXmlParser.get_child_tag_text(preprocess,XmlTags.exe)
            job.preprocess_args = JobXmlParser.get_child_tag_text(preprocess,XmlTags.args)
         postprocess = JobXmlParser.get_child_element(task,XmlTags.postprocess)
         if postprocess:
            job.postprocess = JobXmlParser.get_child_tag_text(postprocess,XmlTags.exe)
            job.postprocess_args = JobXmlParser.get_child_tag_text(postprocess,XmlTags.args)
         
         # parse the input files
         job.input_files = JobXmlParser.get_child_tag_text(task,XmlTags.input_files)

         # parse the output files
         job.output_files = JobXmlParser.get_child_tag_text(task,XmlTags.output_files)

      return job
   
   @staticmethod
   def build_job_xml_element(job_description):
   
      # create the job element (top most element)
      job = ET.Element(XmlTags.job)
      
      # add job_id
      JobXmlParser.add_child_tag(job,XmlTags.job_id,job_description.job_id)

      # add username
      JobXmlParser.add_child_tag(job,XmlTags.user,job_description.user)

      # add transfer in
      JobXmlParser.add_child_tag(job,XmlTags.transfer_in,job_description.transfer_in)

      # add transfer out
      JobXmlParser.add_child_tag(job,XmlTags.transfer_out,job_description.transfer_out)

      # add task details
      task = JobXmlParser.add_child_tag(job,XmlTags.task)

      # add the number of events for this task
      if job_description.num_evts:
         JobXmlParser.add_child_tag(task,XmlTags.nevents,job_description.num_evts)

      # add the number of nodes
      if job_description.nodes:
         JobXmlParser.add_child_tag(task,XmlTags.nodes,job_description.nodes)

      # add the scheduler arguments
      if job_description.scheduler_args:
         JobXmlParser.add_child_tag(task,XmlTags.scheduler_args,job_description.scheduler_args)

      # add the number of cpus per node to use for this job
      if job_description.cpus_per_node:
         JobXmlParser.add_child_tag(task,XmlTags.cpus_per_node,job_description.cpus_per_node)
      
      # add the task duration in minutes for this job
      if job_description.task_duration_minutes:
         JobXmlParser.add_child_tag(task,XmlTags.task_duration_minutes,str(job_description.task_duration_minutes))

      # add program to the task
      program = JobXmlParser.add_child_tag(task,XmlTags.program)
      
      # add the exe and args to the program
      JobXmlParser.add_child_tag(program,XmlTags.exe,job_description.exe)
      JobXmlParser.add_child_tag(program,XmlTags.args,job_description.exe_args)

      # add the preprocess and postprocess to the task
      if job_description.preprocess:
         preprocess = JobXmlParser.add_child_tag(task,XmlTags.preprocess)
         JobXmlParser.add_child_tag(preprocess,XmlTags.exe,job_description.preprocess)
         if job_description.preprocess_args:
            JobXmlParser.add_child_tag(preprocess,XmlTags.args,job_description.preprocess_args)
      if job_description.postprocess:
         postprocess = JobXmlParser.add_child_tag(task,XmlTags.postprocess)
         JobXmlParser.add_child_tag(postprocess,XmlTags.exe,job_description.postprocess)
         if job_description.postprocess_args:
            JobXmlParser.add_child_tag(postprocess,XmlTags.args,job_description.postprocess_args)
      
      # add the input/output files
      if job_description.input_files:
         JobXmlParser.add_child_tag(task,XmlTags.input_files,job_description.input_files)
      if job_description.output_files:
         JobXmlParser.add_child_tag(task,XmlTags.output_files,job_description.output_files)
      

      return job

   @staticmethod
   def write_job_to_xml_file(filename,job_description):
      job = JobXmlParser.build_job_xml_element()
      JobXmlParser.indent(job)
      tree = ET.ElementTree(job)
      tree.write(filename)

   @staticmethod
   def get_job_xml_as_string(job_description):
      job = JobXmlParser.build_job_xml_element()
      string = ''
      try:
         string = ET.tostring(job)
      except TypeError,e:
         logger.error('Failed to parse job description, exception = ' + str(e) + '\n job description: \n' + str(job_description))

      return string

   
   @staticmethod
   def get_job_xml_as_pretty_string(job_description):
      job  = JobXmlParser.build_job_xml_element(job_description)
      JobXmlParser.indent(job)
      string = ''
      try:
         string =  ET.tostring(job)
      except TypeError,e:
         logger.error('Failed to parse job description, exception = ' + str(e) + '\n job description: \n' + str(job_description))

      return string


   '''
   copy and paste from http://effbot.org/zone/element-lib.htm#prettyprint
   it basically walks your tree and adds spaces and newlines so the tree is
   printed in a nice way
   '''
   @staticmethod
   def indent(elem, level=0):
      i = "\n" + level*"  "
      if len(elem):
         if not elem.text or not elem.text.strip():
            elem.text = i + "  "
         if not elem.tail or not elem.tail.strip():
            elem.tail = i
         for elem in elem:
            JobXmlParser.indent(elem, level+1)
         if not elem.tail or not elem.tail.strip():
            elem.tail = i
      else:
         if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

   @staticmethod
   def add_child_tag(parent,tag_name,text = None,attributes = {}):
      new_tag = ET.SubElement(parent,str(tag_name))
      if text is not None:
         new_tag.text = str(text)
      for key in attributes.keys():
         new_tag.set(str(key),str(attributes[key]))

      return new_tag

   @staticmethod
   def get_child_tag_text(parent,child_tag):
      child = JobXmlParser.get_child_element(parent,child_tag)
      if child is None: return None
      return child.text
   
   @staticmethod
   def get_child_tag_text_int(parent,child_tag):
      try:
         text  = JobXmlParser.get_child_tag_text(parent,child_tag)
         value = int(text)   
      except ValueError,e:
         logger.warning('JobXmlParser.get_child_tag_text_int: Tag "' + child_tag + '" is not an integer. From parent "' + parent.tag + '". Tag text is "' + text + '". Exception: ' + str(e))
         return None
      return value

   @staticmethod
   def get_child_element(parent,child_tag):
      child = parent.find(child_tag)
      if child is None:
         logger.warning('JobXmlParser.get_child_element: XML tag "' + child_tag + '" not found in parent element "' + parent.tag + '". May not be expected for this job.')
         return None
      return child

class NoMoreJobs(Exception): pass


