#!/usr/bin/env python
import os,sys,math,logging
from CondorJobDescriptionFile import CondorJobDescriptionFile
logger = logging.getLogger(__name__)

DISALLOWED_CHARACTERS= [';']



class CondorDagmanJobDescriptionFile:
   
   class ReadFileException:
      def __init__(self,filename):
         self.filename = filename
      def __str__(self):
         return ' Failed to open file "' + filename + '".'
   
   def __init__(self,jobs = {},relations = []):
      self.jobs = jobs # dictionary of CondorDagmanJob objects
      self.relations = relations # list of CondorDagmanRelation objects


   def write_file(self,filename):
      try:
         file = open(filename,'w')
      except:
         logger.error('Error opening ' + filename + ' for output.\n')
         raise

      for job in self.jobs.values():  
         file.write(str(job))
      for relation in self.relations:
         file.write(str(relation))
      file.close()

   @staticmethod
   def read_file(filename):
      try:
         file = open(filename,'r')
      except:
         raise CondorDagmanDescriptionFile.ReadFileError(filename)
       
      jobs = {}
      relations = []
      
      for line in file:
         split = line[0:-1].split()
         if len(split) <= 1: continue
         command = split[0].strip()
         if command.upper() == 'JOB':
            job_name = split[1].strip()
            job_filename = split[2].strip()
            if job_name in jobs.keys():
               jobs[job_name].jobfilename = job_filename
            else: # job isn't in jobs list yet so add it
               new_job = CondorDagmanJob(job_name,job_filename)
               jobs[job_name] = new_job
         elif command.upper() == 'VARS':
            job_name = split[1].strip()
            var = split[2].split('=')[0].strip()
            value = split[2].split('=')[1].strip()
            # remove double quotes around the value
            if value[0] == '"': value = value[1:]
            if value[-1] == '"': value = value[0:-1]
            if job_name in jobs.keys():
               jobs[job_name].variables[var] = value
            else: # job isn't in jobs list yet so add it
               new_job = CondorDagmanJob(job_name,None)
               new_job.variables[var] = value
               jobs[job_name] = new_job
         elif command.upper() == 'SCRIPT':
            placement = split[1].strip()
            job_name  = split[2].strip()
            script    = split[3].strip()
            script_args = None
            if len(split) > 4:
               script_args = ''
               for arg in split[4:]:
                  script_args += ' ' + arg
            
            # make sure job exists in jobs list
            job = None
            if job_name in jobs.keys():
               job = jobs[job_name]
            else:
               job = CondorDagmanJob(job_name,None)
               jobs[job_name] = job

            if placement.upper() == 'PRE':
               job.prescript = script
               job.prescript_args = script_args
            elif placement.upper() == 'POST':
               job.postscript = script
               job.postscript = script_args
         elif command.upper() == 'PARENT':
            relations.append(CondorDagmanRelation.from_string(line[0:-1]))
         else:
            logger.warning('Did not parse DAGMAN line: "'+line[0:-1]+'".')

      file.close()

      new_job = CondorDagmanJobDescriptionFile(jobs,relations)
      
      return new_job

   def sanitize(self,exe_path=''):
      # loop over the subjobs and sanitize their commands
      for condor_dagman_subjob in self.jobs.values():
         try:
            # get condor job from dagman subjob
            condor_job = CondorJobDescriptionFile.read_file(condor_dagman_subjob.jobfilename)
            # get the same job but with all variables substituted
            deref_condor_job = condor_dagman_subjob.get_dereferenced_job_desc()
            # copy sensitive options from dereferenced job to non-dereferenced job
            for option in CondorJobDescriptionFile.SANITIZE_OPTIONS:
               condor_job.options[option] = deref_condor_job.options[option]
            # now sanitize the updated condor job, the sanitized job file will also overwrite existing file
            condor_job.sanitize(exe_path,DISALLOWED_CHARACTERS,condor_job.filename) 
         except CondorJobDescriptionFile.ProblemCharacterException:
            logger.exception(' DAGMAN sub job is unsanitary ' )
            raise
         except CondorJobDescriptionFile.ValidExecutableException:
            logger.exception(' DAGMAN sub job could not find executable ')
            raise
         except:
            logger.exception(' DAGMAN sub job exception ')
            raise



class CondorDagmanJob:
   
   def __init__(self,name,jobfilename,variables = {},prescript=None,prescript_args=None,postscript=None,postscript_args=None):
      self.name               = name
      self.jobfilename        = jobfilename
      self.variables          = variables
      self.prescript          = prescript
      self.prescript_args     = prescript_args
      self.postscript         = postscript
      self.postscript_args    = postscript_args
   def __str__(self):
      job_name = ('%' +  str(int(math.ceil(len(self.name)/3)*3)) + 's') % self.name
      s = ('JOB %s %s\n') % (job_name,self.jobfilename)
      for var,value in self.variables.iteritems():
         var_txt = 'VARS %s %s="%s"\n' % (job_name,str(var),str(value))
         var_txt = var_txt.replace('""','"') # check for double quotes
         s += var_txt
      if self.prescript: 
         if self.prescript_args:
            s += 'SCRIPT PRE %s %s %s\n' % (job_name,self.prescript,self.prescript_args)
         else:
            s += 'SCRIPT PRE %s %s\n' % (job_name,self.prescript)
      if self.postscript:
         if self.postscript_args:
            s += 'SCRIPT POST %s %s %s\n' % (job_name,self.postscript,self.postscript_args)
         else:
            s += 'SCRIPT POST %s %s\n' % (job_name,self.postscript)
      return s
   def get_dereferenced_job_desc(self):
      job = CondorJobDescriptionFile.read_file(self.jobfilename)
      # loop over options and dereference variables
      for name,value in job.options.iteritems():
         updated_option_value = value
         variable_names = get_variables(value)
         for variable_name in variable_names:
            try:
               updated_option_value = substitute_variable(variable_name,self.variables[variable_name],updated_option_value)
            except KeyError:
               logger.exception(' Received a KeyError while trying to make a substitution for variable name: ' + str(variable_name) + ' which was not found in dictionary: ' + str(self.variables) )
               raise
            except:
               logger.exception('Failed to substitute variable into job option string; option name: ' + str(name) 
                                + '; option value: ' + str(value) 
                                + ' variable name: ' + str(variable_name) 
                                + ' variable replacement: ' + str(self.variables[variable_name]) 
                                + '  Exception: ' + str(sys.exc_info()[1]) 
                               )
               raise
         job.options[name] = updated_option_value
      return job
      

class CondorDagmanRelation:
   def __init__(self):
      self.parents = [] # list of CondorDagmanJob
      self.children = [] # list of CondorDagmanJob
   def __str__(self):
      s = 'PARENT'
      for parent in self.parents:
         s += ' ' + parent.name
      s = ' CHILD'
      for child in self.children:
         s += ' ' + child.name
      
      return s
   @staticmethod
   def from_string(string_relation):
      split = string_relation.split()
      if split[0] != 'PARENT':
         logger.error('Error parsing Dagman Relation String')
         raise Exception('Error parsing Dagman Relation String')
      
      relation = CondorDagmanRelation()

      for job_name in split[1:]:
         if job_name == 'CHILD': break
         relation.parents.append(job_name)

      for job_name in split[  split.index(relation.parents[len(relation.parents)-1])+2 :]:
         relation.children.append(job_name)

      return relation

def get_variables(parse_string):
   variables = []
   start_index = 0
   while start_index >= 0:
      start_index = parse_string.find('$(',start_index)
      if start_index < 0: continue
      start_index += 2

      end_index = parse_string.find(')',start_index)
      if end_index < 0: continue
      variables.append(parse_string[start_index:end_index])
      start_index = end_index
   return variables

def substitute_variable(name,value,string):
   variable_string = '$(' + name + ')'
   return string.replace(variable_string,str(value))


def sanitize_dagman(input_filename,output_filename):
   try:
      dagman = CondorDagmanJobDescriptionFile.read_file(input_filename)
   except:
      logger.exception('Error loading dagman job')
      raise

   dagman.sanitize()
   
   dagman.write_file(output_filename)


if __name__ == '__main__':
   logging.basicConfig(level=logging.INFO)

   job = CondorDagmanJobDescriptionFile.read_file(sys.argv[1])

   logger.info(' there are ' + str(len(job.jobs)) + ' jobs and ' + str(len(job.relations)) + ' relations.')
   job.write_file('test.dag')

   # dereference one job as a test
   logger.info(' job:\n' + str(job.jobs.values()[0].get_dereferenced_job_desc()))


