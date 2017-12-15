import logging
logger = logging.getLogger(__name__)

class AlpgenOption:
   ''' Class for an Alpgen Option Pair '''
   def __init__(self,label = None,value = None,comment = None):
      self.label = label
      self.value = value
      self.comment = comment
   def get_line(self):
      if self.comment is not None:
         return ("%-15s%15s  !%s" % (self.label,str(self.value),self.comment))
      return ("%-15s%15s" % (self.label,str(self.value)))
   @staticmethod
   def parse_line(line):
      # remove newline character (and only parse first line)
      txt = line.split('\n')[0]
      words = txt.split()
      option = AlpgenOption()
      option.label = words[0]
      option.value = words[1]
      if len(words) > 2 and words[2] == '!':
         option.comment = txt[txt.find('!')+1:]
      return option
         

class AlpgenInputFile:
   ''' Class for configuring alpgen and can write a cmd file '''
      
   def __init__(self):
      # Manditory parameters:
      self.imode           = 0
      self.filename_base   = 'alpout'
      self.start_with      = 0 # 0=new grid file, 1=previous warmup grid file 2= previous generation grid file
      self.nevt            = 100000 # number of events per iteration
      self.nitr            = 2 # number of iterations
      self.last_nevt       = 500000 # number of events for the last warmup step

      # Options for generation
      self.options = []

   def __str__(self):
      txt  = ('%-15s' % str(self.imode))      + '! imode \n'
      txt += ('%-15s' % self.filename_base)   + '! label for files\n'
      txt += ('%-15s' % str(self.start_with)) + '! start with: 0=new grid, 1=previous warmup grid, 2=previous generation grid\n'
      txt += ('%-10s%4s ' % (str(self.nevt),str(self.nitr))) + '! Num events/iteration, Num warm-up iterations\n'
      txt += ('%-15s' % str(self.last_nevt))  + '! Num events generated after warm-up\n'
      txt += '*** The above 5 lines provide mandatory inputs for all processes\n'
      txt += '*** (Comment lines are introduced by the three asteriscs)\n'
      txt += '*** The lines below modify existing defaults for the hard process under study\n'
      txt += '*** For a complete list of accessible parameters and their values,\n'
      txt += '*** input "print 1" (to display on the screen) or "print 2" to write to file\n'

      for option in self.options:
         txt += option.get_line() + '\n'
      
      return txt

   def write(self,filename):
      try:
         file = open(filename,'w')
      except IOError,e:
         logger.error('opening filename '+filename+' for writing. Exception: '+str(e))
         return -1
      
      try:
         file.write(str(self))
      except IOError,e:
         logger.error('writing to filename ' + filename + ' for writing. Exception: ' + str(e))
         return -2

      file.close()
      return 0

   def read(self,filename):
      
      try:
         file = open(filename)
      except IOError,e:
         logger.error('opening filename '+filename+' for reading. Exception: ' + str(e))
         return -3

      counter = 0
      for line in file:
         # remove newline character
         line = line[0:-1]
         
         # skip empty lines
         if len(line.split()) == 0:
            continue

         # skip lines starting with '***'
         if line.split()[0] == '***':
            continue
         
         strings = line.split()
         if counter == 0: #  imode
            try:
               imode = strings[0]
               self.imode = int(imode)
            except ValueError,e:
               logger.error('mode must be an integer, but was '+imode+'. Taken from line:\n' + line + '\n Exception: ' + str(e))
               return -4
            counter += 1
            continue
         elif counter == 1: # filename base
            self.filename_base = strings[0]
            counter += 1
            continue
         elif counter == 2: # start with
            try:
               start_with = strings[0]
               self.start_with = int(start_with)
            except ValueError,e:
               logger.error('start with must be an integer, but was '+start_with+' Taken from line:\n' + line + '\n Exception: ' + str(e))
               return -5
            counter += 1
            continue
         elif counter == 3: # n evevnts per n iterations
            try:
               nevt = strings[0]
               self.nevt = int(nevt)
            except ValueError,e:
               logger.error('number of events per iteration must be an integer, but was '+nevt+' Taken from line:\n' + line + '\n Exception: ' + str(e))
               return -6
            try:
               nitr = strings[1]
               self.nitr = int(nitr)
            except ValueError,e:
               logger.error('number of iterations must be an integer, but was '+nitr+' Taken from line:\n' + line + '\n Exception: ' + str(e))
               return -7
            counter += 1
            continue
         elif counter == 4: # number of weighted events to generate
            try:
               last_nevt = strings[0]
               self.last_nevt = int(last_nevt)
            except ValueError,e:
               logger.error('Number of weighted events to generate must be an integer, but was '+last_nevt+' Taken from line:\n' + line + '\n Exception: ' + str(e))
               return -8
            counter += 1
            continue
         else: # optional values
            if len(strings) >= 2:
               self.options.append(AlpgenOption.parse_line(line))
               continue

      



            




         
