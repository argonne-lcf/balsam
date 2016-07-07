import os,sys,logging
logger = logging.getLogger(__name__)

class CondorJobDescriptionFile:
   UNIVERSE    = 'universe'
   EXECUTABLE  = 'executable'
   ARGUMENTS   = 'arguments'
   LOG         = 'log'
   OUTPUT      = 'output'
   INPUT       = 'input'
   ERROR       = 'error'
   ENVIRONMENT = 'environment'
   PRIORITY    = 'priority'
   PRECMD      = '+precmd'
   PREARGS     = '+preargs'
   POSTCMD     = '+postcmd'
   POSTARGS    = '+postargs'
   SHOULD_TRANSFER_FILES   = 'should_transfer_files'
   WHEN_TO_TRANSFER_OUTPUT = 'when_to_transfer_output'
   TRANSFER_INPUT_FILES    = 'transfer_input_files'
   TRANSFER_OUTPUT_FILES   = 'transfer_output_files'

   SANITIZE_OPTIONS = [EXECUTABLE,ARGUMENTS]
   ENFORCE_EXE_OPTIONS = [EXECUTABLE,PRECMD,POSTCMD]
   
   class ReadFileException(Exception):
      def __init__(self,filename,added_info):
         self.filename = filename
         self.added_info = added_info
      def __str__(self):
         return 'Failed to create CondorJobDescriptionFile from input file: ' + str(self.filename) + '. ' + str(self.added_info)
   class ProblemCharacterException(Exception):
      def __init__(self,character,option,value):
         self.character = character
         self.option = option
         self.value = value
      def __str__(self):
         return 'Character "'+str(self.character)+'" is not allowed in condor description for option "'+str(self.option)+'" which was set to "'+str(self.value)+'".'
   class ValidExecutableException(Exception):
      def __init__(self,exe,exe_dir):
         self.exe = exe
         self.exe_dir = exe_dir
      def __str__(self):
         return 'Executable "'+str(self.exe)+'" not found in allowed executable directory "'+str(self.exe_dir)+'".'

   
   def __init__(self,
               universe                = 'vanilla',
               executable              = None,
               arguments               = None,
               log                     = 'condor_log.txt',
               input                   = None,
               output                  = None,
               error                   = None,
               environment             = None,
               priority                = 5,
               should_transfer_files   = 'YES',
               when_to_transfer_output = 'ON_EXIT',
               transfer_input_files    = None,
               transfer_output_files   = None,
               ):
      self.options = {}
      self.options[self.UNIVERSE]                 = universe
      self.options[self.EXECUTABLE]               = executable
      self.options[self.ARGUMENTS]                = arguments
      self.options[self.LOG]                      = log
      self.options[self.INPUT]                    = input
      self.options[self.OUTPUT]                   = output
      self.options[self.ERROR]                    = error
      self.options[self.ENVIRONMENT]              = environment
      self.options[self.PRIORITY]                 = priority
      self.options[self.SHOULD_TRANSFER_FILES]    = should_transfer_files
      self.options[self.WHEN_TO_TRANSFER_OUTPUT]  = when_to_transfer_output
      self.options[self.TRANSFER_INPUT_FILES]     = transfer_input_files
      self.options[self.TRANSFER_OUTPUT_FILES]    = transfer_output_files

      self.filename = None


   def write_file(self,filename):
      try:
         file = open(filename,'w')
      except:
         logger.error(' Error opening ' + filename + ' for output.')
         raise
      file.write(str(self))
      file.write('queue')
      file.close()
      self.filename = filename

   def __str__(self):
      s = ''
      for option,value in self.options.iteritems():
         s += '%-30s = %-s\n' % (str(option),str(value))
      return s
   
   def sanitize(self,exe_path='',flag_char_list=[';'],filename=None):
      for option in self.SANITIZE_OPTIONS:
         value = self.options[option]
         for flag_char in flag_char_list:
            if value.find(flag_char) >= 0:
               logger.error('Found flag character, "' + flag_char + '", while sanitizing option "' + option + '": ' + value)
               raise self.ProblemCharacterException( flag_char,option,value)
      
      # for exectuables ensure they fall in exe_path
      for option in self.ENFORCE_EXE_OPTIONS:
         exe = os.path.join(exe_path,os.path.basename(self.options[option]).strip())
         # append allowed directory
         if os.path.isfile(exe):
            self.options[option] = exe
         else:
            logger.error('Executable, "' + exe + '", not found.')
            raise self.ValidExecutableException(exe,exe_path)

      # overwrite job file
      if filename:
         self.write_file(filename)
         

   @staticmethod
   def read_file(filename):
      new_file = CondorJobDescriptionFile()
      new_file.options = {}
      try:
         file = open(filename,'r')
      except:
         logger.error(' Error opening ' + filename + ' for input.')
         raise CondorJobDescriptionFile.ReadFileException(filename,'Error opening file.')
      
      for line in file.readlines():
         index = line.find('=') # option = value
         if index < 0: continue
         option = line[0:index].strip().lower() # remove spaces and make all lowercase
         if option == 'queue': # Queue is usually the last thing in the file
            break
         value = line[index+1:].replace('\n','').strip()
         new_file.options[option] = value
      file.close()
      
      new_file.filename = filename

      return new_file
