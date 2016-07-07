#!/usr/bin/env python
import sys,logging,optparse

from AlpgenArgoJob import AlpgenArgoJob
sys.path.append('/users/hpcusers/balsam/argo_deploy/argo_core')
from MessageInterface import MessageInterface

def main():
   parser = optparse.OptionParser(description='submit alpgen job to ARGO')
   parser.add_option('-e','--evts-per-iter',dest='evts_per_iter',help='number of events per warmup iteration',type='int')
   parser.add_option('-i','--num-iter',dest='numiters',help='number of iterations for the warmup',type='int')
   parser.add_option('-w','--warmup-weighted',dest='num_warmup',help='number of event to in the warmup, after the iterations complete',type='int')
   parser.add_option('-n','--num-weighted',dest='num_weighted',help='number of weighted events to generate.',type='int')
   parser.add_option('-p','--process',dest='process',help='define the process to generate, 2Q,4Q,hjet,top,wjet,zjet,Njet,etc.')
   parser.add_option('-o','--num-nodes',dest='numnodes',help='number of nodes to use on destination machine',type='int')
   parser.add_option('-c','--cpus-per-node',dest='cpus_per_node',help='number of CPUs per node to use on destination machine',type='int')
   parser.add_option('-a','--alpgen-input',dest='alpgen_input_file',help='The AlpGen input file which carries all the options for this generation job')
   parser.add_option('-t','--wall-time',dest='walltime',help='The wall time to submit to the queue in minutes.',type='int')
   options,args = parser.parse_args()


   if options.numiters is None:
      parser.error('Must define the number of warmup iterations')
   if options.process is None:
      parser.error('Must define the process to generate')
   if options.numnodes is None:
      parser.error('Must define the number of nodes to use')
   if options.cpus_per_node is None:
      parser.error('Must define the number of CPUs per node to use')
   if options.evts_per_iter is None:
      parser.error('Must define the number of events per warmup iteration')
   if options.num_weighted is None:
      parser.error('Must define the number of weighted events to produce')
   if options.num_warmup is None:
      parser.error('Must define the number of weighted events to produce in the warmup step.')
   if options.alpgen_input_file is None:
      parser.error('Must define the AlpGen input file')
   if options.walltime is None:
      parser.error('Must specify a wall time')
   
   
   user = os.environ.get('USER','nobody')
   if(user == 'apf'): # AutoPyFactory
      user= os.environ.get('prodUserID','nobody')
   jobID = taskID + '0'

   if options.resubmitjobid is not None:
      jobID = int(options.resubmitjobid)

   TOP_PATH = os.getcwd() # directory in which script was run
   RUNPATH = os.path.join(TOP_PATH,str(jobID)) # directory in which to store files
   if not os.path.exists(RUNPATH):
      os.makedirs(RUNPATH) # make directories recursively like 'mkdir -p'

   logger.info('JobID: ' + str(jobID))


if __name__ == '__main__':
   main()

