#!/usr/bin/env python
import sys,logging,optparse

from AlpgenArgoJob import AlpgenArgoJob
sys.path.append('/users/hpcusers/balsam/argo_deploy/argo_core')
from MessageInterface import MessageInterface

def main():
   mi = MessageInterface(host='atlasgridftp02.hep.anl.gov',
                         port=5671,
                         exchange_name='argo_users',
                         ssl_cert='/users/hpcusers/balsam/gridsecurity/jchilders/xrootdsrv-cert.pem',
                         ssl_key='/users/hpcusers/balsam/gridsecurity/jchilders/xrootdsrv-key.pem',
                         ssl_ca_certs='/users/hpcusers/balsam/gridsecurity/jchilders/cacerts.pem',
                        )

   mi.open_blocking_connection()

   body = '''
{
   "preprocess": null,
   "preprocess_args": null,
   "postprocess": null,
   "postprocess_args": null,
   "input_url":"/grid/atlas/hpc/transfer/from_hpc/14005151768701230",
   "output_url":"/grid/atlas/hpc/transfer/to_hpc/14005151768701230",
   "username": "jchilders",
   "jobs":[
      {
       "exe": "zjetgen90",
       "exe_args": "input.0",
       "input_files": ["alpout.input.0","cteq6l1.tbl"],
       "nodes": 1,
       "num_evts": -1,
       "output_files": ["alpout.grid1,alpout.grid2"],
       "postprocess": null,
       "postprocess_args": null,
       "preprocess": null,
       "preprocess_args": null,
       "processes_per_node": 1,
       "scheduler_args": null,
       "wall_minutes": 60
       },

      {
       "exe": "alpgenCombo.sh",
       "exe_args": "zjetgen90_mpi alpout.input.1 alpout.input.2",
       "input_files": ["alpout.input.1","alpout.input.2","cteq6l1.tbl","alpout.grid1","alpout.grid2"],
       "nodes": 1,
       "num_evts": -1,
       "output_files": ["alpout.unw","alpout_unw.par","alpout.wgt","alpout.par","directoryList_before.txt","directoryList_after.txt","postsubmit.err","postsubmit.out"],
       "postprocess": "postsubmit.sh",
       "postprocess_args": "alpout",
       "preprocess": "presubmit.sh",
       "preprocess_args": null,
       "processes_per_node": 16,
       "scheduler_args": "--mode=script",
       "wall_minutes": 60
      }
   ]
}
'''
   mi.send_msg(body,'argo_job')

   
if __name__ == '__main__':
   main()

