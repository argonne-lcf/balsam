import sys,logging,shutil
logger = logging.getLogger(__name__)
from AlpgenInputFile import AlpgenInputFile
sys.path.append('/users/hpcusers/balsam/argo_deploy/argo_core')
from ArgoJob import ArgoJob
from BalsamJob import BalsamJob
import GridFtp

class AlpgenArgoJob:
   INPUT_FILE_POSTFIX_IMODE0 = '.input.0'
   INPUT_FILE_POSTFIX_IMODE1 = '.input.1'
   INPUT_FILE_POSTFIX_IMODE2 = '.input.2'

   EVTGEN_SCHEDULER_ARGS     = '--mode=script'
   EVTGEN_EXECUTABLE         = 'alpgenCombo.sh'

   def __init__(self,
                executable                      = None,
                input_filename                  = None,
                warmup_phase0_number_events     = None,
                warmup_phase0_number_iterations = None,
                warmup_phase1_number_events     = None,
                warmup_wall_minutes             = None,
                evtgen_phase0_number_events     = None,
                evtgen_phase0_number_iterations = None,
                evtgen_phase1_number_events     = None,
                evtgen_nodes                    = None,
                evtgen_processes_per_node       = None,
                evtgen_wall_minutes             = None,
                working_path                    = None,
                input_url                       = None,
                output_url                      = None,
                pdf_filename                    = None,
                username                        = None,
               ):
      self.executable                        = executable
      self.input_filename                    = input_filename
      self.warmup_phase0_number_events       = warmup_phase0_number_events
      self.warmup_phase0_number_iterations   = warmup_phase0_number_iterations
      self.warmup_phase1_number_events       = warmup_phase1_number_events
      self.warmup_wall_minutes               = warmup_wall_minutes
      self.evtgen_phase0_number_events       = evtgen_phase0_number_events
      self.evtgen_phase0_number_iterations   = evtgen_phase0_number_iterations
      self.evtgen_phase1_number_events       = evtgen_phase1_number_events
      self.evtgen_nodes                      = evtgen_nodes
      self.evtgen_processes_per_node         = evtgen_processes_per_node
      self.evtgen_wall_minutes               = evtgen_wall_minutes
      self.working_path                      = working_path
      self.input_url                         = input_url
      self.output_url                        = output_url
      self.pdf_filename                      = pdf_filename
      self.username                          = username

   def get_argo_job(self):
      ##-----------------------
      # setup input files
      ##-----------------------
      
      # load input file
      input = AlpgenInputFile()
      input.read(self.input_filename)
      filename_base = input.filename_base
      
      # create input for imode 0
      input.imode             = 0
      input.start_with        = 0
      input.nevt              = self.warmup_phase0_number_events
      input.nitr              = self.warmup_phase0_number_iterations
      input.last_nevt         = self.warmup_phase1_number_events
      input_filename_imode0   = os.path.join(self.working_path,filename_base + INPUT_FILE_POSTFIX_IMODE0)
      input.write(input_filename_imode0)
      
      # create input for imode 1
      input.imode             = 1
      input.start_with        = 2
      input.nevt              = self.evtgen_phase0_number_events
      input.nitr              = self.evtgen_phase0_number_iterations
      input.last_nevt         = self.evtgen_phase1_number_events
      input_filename_imode1   = os.path.join(self.working_path,filename_base + INPUT_FILE_POSTFIX_IMODE1)
      input.write(input_filename_imode1)
      
      # create input for imode 2
      input.imode             = 2
      input.start_with        = 1
      input.nevt              = 0
      input.nitr              = 0
      input.last_nevt         = 0
      input_filename_imode2   = os.path.join(self.working_path,filename_base + INPUT_FILE_POSTFIX_IMODE2)
      input.write(input_filename_imode2)
      
      # copy pdf file to working path
      try:
         os.copy(self.pdf_filename,self.working_path + '/')
      except:
         logger.exception(' received exception while copying PDF file: ' + str(sys.exc_info()[1]))
         raise

      # copy files to grid ftp location
      try:
         GridFtp.globus_url_copy(self.working_path + '/',self.input_url + '/')
      except:
         logger.exception(' received exception while copying working path to grid ftp input path: ' + str(sys.exc_info()[1]))
         raise
      
      grid1 = filename_base + '.grid1'
      grid2 = filename_base + '.grid2'
      
      # create warmup balsam job
      warmup = BalsamJob()
      warmup.executable          = self.executable
      warmup.exectuable_args     = input_filname_imode0
      warmup.input_files         = [input_filename_imode0,self.pdf_filename]
      warmup.output_files        = [grid1,grid2]
      warmup.nodes               = 1
      warmup.processes_per_node  = 1
      warmup.wall_minutes        = self.warmup_wall_minutes
      warmup.username            = self.username

      # create event gen balsam job
      evtgen = BalsamJob()
      evtgen.executable          = EVTGEN_EXECUTABLE
      evtgen.exectuable_args     = self.exectuable + ' ' + input_filename_imode1 + ' ' + input_filename_imode2
      evtgen.input_files         = [grid1,grid2,input_filename_imode1,input_filename_imode2,self.pdf_filename]
      evtgen.output_files        = [filename_base + '.unw',
                                    filename_base + '_unw.par',
                                    filename_base + '.wgt',
                                    filename_base + '.par',
                                    'directoryList_before.txt',
                                    'directoryList_after.txt',
                                   ]
      evtgen.preprocess          = 'presubmit.sh'
      evtgen.postprocess         = 'postsubmit.sh'
      evtgen.postprocess_args    = filename_base
      evtgen.nodes               = self.evtgen_nodes
      evtgen.processes_per_node  = self.evtgen_processes_per_node
      evtgen.wall_minutes        = self.evtgen_wall_minutes
      evtgen.username            = self.username
      evtgen.scheduler_args      = EVTGEN_SCHEDULER_ARGS
      
      
      argojob = ArgoJob()
      argojob.input_url          = self.input_url
      argojob.output_url         = self.output_url
      argojob.username           = self.username
      argojob.add_job(warmup)
      argojob.add_job(evtgen)

      return argojob

