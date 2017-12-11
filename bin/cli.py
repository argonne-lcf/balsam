# These must come before any other imports
import django
import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
django.setup()
# --------------
import argparse
import sys
from cli_commands import newapp,newjob,newdep,ls,modify,rm,qsub
from cli_commands import kill,mkchild,launcher,service,make_dummies
from django.conf import settings

def main():
    parser = make_parser()
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    args.func(args)


def make_parser():
    parser = argparse.ArgumentParser(prog='balsam', description="Balsam command line interface")
    subparsers = parser.add_subparsers(title="Subcommands")


    # ADD APP
    # --------
    parser_app = subparsers.add_parser('app',
                                       help="add a new application definition",
                                       description="add a new application definition",
                                       )
    parser_app.set_defaults(func=newapp)
    parser_app.add_argument('--name', required=True)
    parser_app.add_argument('--description', nargs='+', required=True)
    parser_app.add_argument('--executable', help='full path to executable', 
                            required=True)
    parser_app.add_argument('--preprocess', default='', 
                            help='preprocessing script with full path')
    parser_app.add_argument('--postprocess', default='',
                            help='postprocessing script with full path')
    parser_app.add_argument('--env', action='append', default=[], 
                            help="Environment variables specific " 
                            "to this app; specify multiple envs like " 
                            "'--env VAR1=VAL1 --env VAR2=VAL2'.  ")
    # -------------------------------------------------------------------


    # ADD JOB
    # -------
    BALSAM_SITE = settings.BALSAM_SITE
    parser_job = subparsers.add_parser('job',
                                       help="add a new Balsam job",
                                       description="add a new Balsam job",
                                       )
    parser_job.set_defaults(func=newjob)

    parser_job.add_argument('--name', required=True)
    parser_job.add_argument('--workflow', required=True,
                            help="A workflow name for grouping related jobs")
    parser_job.add_argument('--application', help='Name of the '
                            'application to use; must exist in '
                            'ApplicationDefinition DB', required=True)
    
    parser_job.add_argument('--wall-minutes', type=int, required=True)
    parser_job.add_argument('--num-nodes',
                            type=int, required=True)
    parser_job.add_argument('--processes-per-node',
                            type=int, required=True)
    
    parser_job.add_argument('--allowed-site', action='append',
                            required=False, default=[BALSAM_SITE],
                            help="Balsam instances where this job can run; "
                            "defaults to the local Balsam instance")

    parser_job.add_argument('--description', required=False, nargs='*',
                            default=[])

    parser_job.add_argument('--threads-per-rank',type=int, default=1,
                            help="Equivalent to -d option in aprun")
    parser_job.add_argument('--threads-per-core',type=int, default=1,
                            help="Equivalent to -j option in aprun")

    parser_job.add_argument('--args', nargs='*', required=False, default=[],
                            help="Command-line args to the application")
    parser_job.add_argument('--preprocessor', required=False, default='',
                            help="Override application-defined preprocess")
    parser_job.add_argument('--postprocessor', required=False, default='',
                            help="Override application-defined postprocess")
    parser_job.add_argument('--post_handle_error', action='store_true',
                            help="Flag enables job runtime error handling by "
                            "postprocess script")
    parser_job.add_argument('--post_handle_timeout', action='store_true',
                            help="Flag enables job timeout handling by "
                            "postprocess script")
    parser_job.add_argument('--disable_auto_timeout_retry', action='store_true',
                            help="Flag disables automatic job retry if it has "
                            "timed out in a previous run")

    parser_job.add_argument('--input-files', nargs='*', required=False, 
                            default=['*'], help="Dataflow: filename patterns "
                            "that will be searched for in the parent job "
                            "working directories and retrieved for input. "
                            "[Ex: '*.log gopt.dat geom???.xyz' ]")

    parser_job.add_argument('--url-in', required=False,default='',
                            help='Input URL from which remote input files are copied.')
    parser_job.add_argument('--url-out',required=False,default='',
                            help='Output URL to which output files are copied.')
    parser_job.add_argument('--stage-out-files', nargs='*', required=False,default=[],
                            help="Filename patterns; matches will be "
                            "transferred to the destination specified " 
                            "by --url-out option")
    parser_job.add_argument('--env', action='append', required=False,
                            default=[], help="Environment variables specific " 
                            "to this job; specify multiple envs like " 
                            "'--env VAR1=VAL1 --env VAR2=VAL2'.  "
                            "Application-specific variables can instead be "
                            "given in the ApplicationDefinition to avoid "
                            "repetition here.")

    parser_job.add_argument('--yes', help='Skip prompt confirming job details.',
                            required=False,action='store_true')
    #--------------------------------------------------------------------


    # ADD DEP
    # -------
    parser_dep = subparsers.add_parser('dep',
                                       help="add a dependency between two existing jobs",
                                       description="add a dependency between two existing jobs"
                                       )
    parser_dep.set_defaults(func=newdep) 
    parser_dep.add_argument('parent', help="Parent must be finished before child")
    parser_dep.add_argument('child', help="The dependent job")
    #-------------------------------------------------------------------------


    # LS
    # ----
    parser_ls = subparsers.add_parser('ls', help="list jobs, applications, or jobs-by-workflow")
    parser_ls.set_defaults(func=ls)
    parser_ls.add_argument('objects', choices=['jobs', 'apps', 'wf'], default='jobs',
                           nargs='?', help="list all jobs, all apps, or jobs by workflow")
    parser_ls.add_argument('--name', help="match any substring of job name")
    parser_ls.add_argument('--history', help="show state history", action='store_true')
    parser_ls.add_argument('--id', help="match any substring of job id")
    parser_ls.add_argument('--wf', help="Filter jobs matching a workflow")
    parser_ls.add_argument('--verbose', action='store_true')
    parser_ls.add_argument('--tree', action='store_true', help="show DAG in tree format")
    # -----------------------------------------------------------


    # MODIFY
    # ------
    parser_modify = subparsers.add_parser('modify', help="alter job or application")
    parser_modify.set_defaults(func=modify)
    parser_modify.add_argument('obj_type', choices=['jobs', 'apps'])
    parser_modify.add_argument('id', help="substring of job or app ID to match")
    parser_modify.add_argument('--attr', help="attribute of job or app to modify",
                               required=True)
    parser_modify.add_argument('--value', help="modify attr to this value",
                               required=True)
    # -----------------------------------------------------------------------

    
    # RM
    # --
    parser_rm = subparsers.add_parser('rm', help="remove jobs or applications from the database")
    parser_rm.set_defaults(func=rm)
    parser_rm.add_argument('objects', choices=['jobs', 'apps'], help="permanently delete jobs or apps from DB")
    parser_rm.add_argument('--force', action='store_true', help="force delete")
    group = parser_rm.add_mutually_exclusive_group(required=True)
    group.add_argument('--name', help="match any substring of job name")
    group.add_argument('--id', help="match any substring of job id")
    group.add_argument('--all', action='store_true', help="delete all objects in the DB")
    # --------------------------------------------------------------------------------------------------


    # QSUB
    # ----
    parser_qsub = subparsers.add_parser('qsub', help="add a one-line job to the database, bypassing Application table")
    parser_qsub.set_defaults(func=qsub)
    parser_qsub.add_argument('command', nargs='+')
    parser_qsub.add_argument('-n', '--nodes', type=int, default=1)
    parser_qsub.add_argument('-N', '--ppn', type=int, default=1)
    parser_qsub.add_argument('--name', default='')
    parser_qsub.add_argument('-t', '--wall-minutes', type=int, required=True)
    parser_qsub.add_argument('-d', '--threads-per-rank',type=int, default=1)
    parser_qsub.add_argument('-j', '--threads-per-core',type=int, default=1)
    parser_qsub.add_argument('--env', action='append', required=False, default=[])
    # --------------------------------------------------------------------------------------------------


    # KILL
    # ----
    parser_kill = subparsers.add_parser('killjob', help="Kill a job without removing it from the DB")
    parser_kill.set_defaults(func=kill)
    parser_kill.add_argument('--id', required=True)
    parser_kill.add_argument('--recursive', action='store_true')
    # -----------------------------------------------------


    # MAKECHILD
    # ---------
    parser_mkchild = subparsers.add_parser('mkchild', help="Create a child job of a specified job")
    parser_mkchild.set_defaults(func=mkchild)
    parser_mkchild.add_argument('--name', required=True)
    parser_mkchild.add_argument('--workflow', required=True,
                            help="A workflow name for grouping related jobs")
    parser_mkchild.add_argument('--application', help='Name of the '
                            'application to use; must exist in '
                            'ApplicationDefinition DB', required=True)
    
    parser_mkchild.add_argument('--wall-minutes', type=int, required=True)
    parser_mkchild.add_argument('--num-nodes',
                            type=int, required=True)
    parser_mkchild.add_argument('--processes-per-node',
                            type=int, required=True)
    
    parser_mkchild.add_argument('--allowed-site', action='append',
                            required=False, default=[BALSAM_SITE],
                            help="Balsam instances where this job can run; "
                            "defaults to the local Balsam instance")

    parser_mkchild.add_argument('--description', required=False, nargs='*',
                            default=[])

    parser_mkchild.add_argument('--threads-per-rank',type=int, default=1,
                            help="Equivalent to -d option in aprun")
    parser_mkchild.add_argument('--threads-per-core',type=int, default=1,
                            help="Equivalent to -j option in aprun")

    parser_mkchild.add_argument('--args', nargs='*', required=False, default=[],
                            help="Command-line args to the application")
    parser_mkchild.add_argument('--preprocessor', required=False, default='',
                            help="Override application-defined preprocess")
    parser_mkchild.add_argument('--postprocessor', required=False, default='',
                            help="Override application-defined postprocess")
    parser_mkchild.add_argument('--post_handle_error', action='store_true',
                            help="Flag enables job runtime error handling by "
                            "postprocess script")
    parser_mkchild.add_argument('--post_handle_timeout', action='store_true',
                            help="Flag enables job timeout handling by "
                            "postprocess script")
    parser_mkchild.add_argument('--disable_auto_timeout_retry', action='store_true',
                            help="Flag disables automatic job retry if it has "
                            "timed out in a previous run")

    parser_mkchild.add_argument('--input-files', nargs='*', required=False, 
                            default=['*'], help="Dataflow: filename patterns "
                            "that will be searched for in the parent job "
                            "working directories and retrieved for input. "
                            "[Ex: '*.log gopt.dat geom???.xyz' ]")

    parser_mkchild.add_argument('--url-in', required=False,default='',
                            help='Input URL from which remote input files are copied.')
    parser_mkchild.add_argument('--url-out',required=False,default='',
                            help='Output URL to which output files are copied.')
    parser_mkchild.add_argument('--stage-out-files', nargs='*', required=False,default=[],
                            help="Filename patterns; matches will be "
                            "transferred to the destination specified " 
                            "by --url-out option")
    parser_mkchild.add_argument('--env', action='append', required=False,
                            default=[], help="Environment variables specific " 
                            "to this job; specify multiple envs like " 
                            "'--env VAR1=VAL1 --env VAR2=VAL2'.  "
                            "Application-specific variables can instead be "
                            "given in the ApplicationDefinition to avoid "
                            "repetition here.")

    parser_mkchild.add_argument('--yes', help='Skip prompt confirming job details.',
                            required=False,action='store_true')
    # -----------------------------------------------------


    # LAUNCHER
    # --------
    parser_launcher = subparsers.add_parser('launcher', help="Start an instance of the balsam launcher")
    group = parser_launcher.add_mutually_exclusive_group(required=True)
    group.add_argument('--job-file', help="File of Balsam job IDs")
    group.add_argument('--consume-all', action='store_true', 
                        help="Continuously run all jobs from DB")
    group.add_argument('--wf-name',
                       help="Continuously run jobs of specified workflow")
    parser_launcher.add_argument('--num-workers', type=int, default=1,
                        help="Theta: defaults to # nodes. BGQ: the # of subblocks")
    parser_launcher.add_argument('--nodes-per-worker', type=int, default=1,
                        help="For non-MPI jobs, how many to pack per worker")
    parser_launcher.add_argument('--max-ranks-per-node', type=int, default=1)
    parser_launcher.add_argument('--time-limit-minutes', type=float,
                        help="Override auto-detected walltime limit (runs"
                        " forever if no limit is detected or specified)")
    parser_launcher.add_argument('--daemon', action='store_true')
    parser_launcher.set_defaults(func=launcher)
    # -----------------


    # SERVICE
    # -------
    parser_service = subparsers.add_parser('service', 
                                           help="Start an instance of the balsam metascheduler service")
    parser_service.set_defaults(func=service)
    # -------------------------

    # DUMMIES
    # ---------
    parser_dummy = subparsers.add_parser('make_dummies')
    parser_dummy.add_argument('num', type=int)
    parser_dummy.set_defaults(func=make_dummies)

    return parser


if __name__ == "__main__":
    main()
