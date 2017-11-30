# These must come before any other imports
import django
import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
django.setup()
# --------------
import argparse
import sys
from cli_commands import newapp,newjob,newdep,ls,modify,rm,qsub
from cli_commands import kill,mkchild,launcher,service
from django.conf import settings


def make_parser():
    parser = argparse.ArgumentParser(prog='balsam', description="Balsam command line interface")
    subparsers = parser.add_subparsers(title="Subcommands")


    # Add app
    parser_app = subparsers.add_parser('app',
                                       help="add a new application definition",
                                       description="add a new application definition",
                                       )
    parser_app.set_defaults(func=newapp)
    parser_app.add_argument('-n','--name',dest='name',
                            help='application name',required=True)
    parser_app.add_argument('-d','--description',dest='description',
                            help='application description',required=True)
    parser_app.add_argument('-e','--executable',dest='executable',
                            help='application executable with full path',required=True)
    parser_app.add_argument('-r','--preprocess',dest='preprocess',
                            help='preprocessing script with full path', default='')
    parser_app.add_argument('-o','--postprocess',dest='postprocess',
                            help='postprocessing script with full path', default='')


    # Add job
    parser_job = subparsers.add_parser('job',
                                       help="add a new Balsam job",
                                       description="add a new Balsam job",
                                       )
    parser_job.set_defaults(func=newjob)
    parser_job.add_argument('-e','--name',dest='name',type=str,
                            help='job name',required=True)
    parser_job.add_argument('-d','--description',dest='description',type=str,
                            help='job description',required=False,default='')

    parser_job.add_argument('-t','--wall-minutes',dest='wall_time_minutes',type=int,
                            help='estimated job walltime in minutes',required=True)

    parser_job.add_argument('-n','--num-nodes',dest='num_nodes',type=int,
                            help='number of nodes to use',required=True)
    parser_job.add_argument('-p','--processes-per-node',dest='processes_per_node',type=int,
                            help='number of processes to run on each node',required=True)
    parser_job.add_argument('-m','--threads-per-rank',dest='threads_per_rank',type=int,
                            default=1)
    parser_job.add_argument('-m','--threads-per-core',dest='threads_per_core',type=int,
                            default=1)

    parser_job.add_argument('-a','--application',dest='application',type=str,
                            help='Name of the application to use; must exist in ApplicationDefinition DB',
                            required=True)

    parser_job.add_argument('-i','--input-url',dest='input_url',type=str,
                            help='Input URL from which input files are copied.',required=False,default='')
    parser_job.add_argument('-o','--output-url',dest='output_url',type=str,
                            help='Output URL to which output files are copied.',required=False,default='')
    parser_job.add_argument('-y',dest='yes',
                            help='Skip prompt confirming job details.',required=False,action='store_true')

    # Add dep
    parser_dep = subparsers.add_parser('dep',
                                       help="add a dependency between two existing jobs",
                                       description="add a dependency between two existing jobs"
                                       )
    parser_dep.set_defaults(func=newdep) 
    parser_dep.add_argument('parent', help="Parent must be finished before child")
    parser_dep.add_argument('child', help="The dependent job")

    # ls
    parser_ls = subparsers.add_parser('ls', help="list jobs, applications, or jobs-by-workflow")
    parser_ls.set_defaults(func=ls)
    parser_ls.add_argument('object', choices=['jobs', 'apps', 'wf'],
                           help="list all jobs, all apps, or jobs by workflow")

    # modify
    parser_modify = subparsers.add_parser('modify', help="alter job or application")
    parser_modify.set_defaults(func=modify)
    
    # rm
    parser_rm = subparsers.add_parser('rm', help="remove jobs or applications from the database")
    parser_rm.set_defaults(func=rm)

    # qsub
    parser_qsub = subparsers.add_parser('qsub', help="add a one-line job to the database, bypassing Application table")
    parser_qsub.set_defaults(func=qsub)

    # kill
    parser_kill = subparsers.add_parser('killjob', help="Kill a job without removing it from the DB")
    parser_kill.set_defaults(func=kill)

    # makechild
    parser_mkchild = subparsers.add_parser('mkchild', help="Create a child job of a specified job")
    parser_mkchild.set_defaults(func=mkchild)

    # launcher
    parser_launcher = subparsers.add_parser('launcher', help="Start an instance of the balsam launcher")
    parser_launcher.set_defaults(func=launcher)

    # service
    parser_service = subparsers.add_parser('service', 
                                           help="Start an instance of the balsam metascheduler service")
    parser_service.set_defaults(func=service)

    return parser

if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    print(args)
    args.func(args)
