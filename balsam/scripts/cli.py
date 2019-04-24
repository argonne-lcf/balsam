# These must come before any other imports
# --------------
import argparse
import sys
from balsam.scripts.cli_commands import newapp,newjob,newdep,ls,modify,rm
from balsam.scripts.cli_commands import kill,mkchild,launcher,service,make_dummies
from balsam.scripts.cli_commands import init, which, server, submitlaunch, log
from balsam import __version__

def main():
    if not sys.version_info >= (3,6):
        sys.stderr.write("Balsam requires Python version >= 3.6\n")
        sys.exit(1)
    parser = make_parser()
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    args.func(args)

def config_launcher_subparser(subparser=None):
    if subparser is None:
        parser = argparse.ArgumentParser(description="Start Balsam Job Launcher.")
    else:
        parser = subparser

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--consume-all', action='store_true', help="Continuously run all jobs from DB")
    group.add_argument('--wf-filter', help="Continuously run jobs of specified workflow")
    parser.add_argument('--job-mode', choices=['mpi', 'serial'],
            required=True, default='mpi')
    parser.add_argument('--time-limit-minutes', type=float, default=0, 
                        help="Provide a walltime limit if not already imposed")
    parser.add_argument('--num-transition-threads', type=int, default=None)
    parser.add_argument('--gpus-per-node', type=int, default=None)
    return parser

def service_subparser(subparser=None):
    if subparser is None:
        parser = argparse.ArgumentParser(description="Start Balsam Job Launcher.")
    else:
        parser = subparser
    return parser

def make_parser():
    parser = argparse.ArgumentParser(prog='balsam', description="Balsam "+__version__)
    subparsers = parser.add_subparsers(title="Command line interface")


    # ADD APP
    # --------
    parser_app = subparsers.add_parser('app',
                                       help="add a new application definition",
                                       description="add a new application definition",
                                       )
    parser_app.set_defaults(func=newapp)
    parser_app.add_argument('--name', required=True)
    parser_app.add_argument('--executable', help='full path to executable', 
                            required=True)
    parser_app.add_argument('--preprocess', default='', 
                            help='preprocessing script with full path')
    parser_app.add_argument('--postprocess', default='',
                            help='postprocessing script with full path')
    parser_app.add_argument('--description', nargs='+')
    # -------------------------------------------------------------------


    # ADD JOB
    # -------
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
    
    parser_job.add_argument('--wall-time-minutes', type=int, required=False, default=1)
    parser_job.add_argument('--num-nodes',
                            type=int, required=False,default=1,
                            help='Number of compute nodes on which to run MPI '
                            'job. (Total number of MPI ranks determined as '
                            'num_nodes * ranks_per_node).')
    parser_job.add_argument('--coschedule-num-nodes',
                            type=int, required=False,default=0)
    parser_job.add_argument('--node-packing-count',
                            type=int, required=False,default=1)
                            
    parser_job.add_argument('--ranks-per-node',
                            type=int, required=False,default=1,
                            help='Number of MPI ranks per compute node. '
                            '(Total MPI ranks calculated from num_nodes * '
                            'ranks_per_node. If only 1 total ranks, treated as serial '
                            'job).')
    
    parser_job.add_argument('--description', required=False, nargs='*', default=[])

    parser_job.add_argument('--threads-per-rank',type=int, default=1,
                            help="Equivalent to -d option in aprun")
    parser_job.add_argument('--threads-per-core',type=int, default=1,
                            help="Equivalent to -j option in aprun")

    parser_job.add_argument('--args', nargs='*', required=False, default=[],
                            help="Command-line args to the application")
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
    parser_ls.add_argument('objects', choices=['jobs', 'apps', 'wf', 'queues'], default='jobs',
                           nargs='?', help="list all jobs, all apps, or jobs by workflow")
    parser_ls.add_argument('--name', help="match any substring of job name")
    parser_ls.add_argument('--history', help="show state history / logs", action='store_true')
    parser_ls.add_argument('--id', help="match any substring of job id")
    parser_ls.add_argument('--state', help="list jobs matching a state")
    parser_ls.add_argument('--by-states', action='store_true', help="group job listing by states")
    parser_ls.add_argument('--wf', help="Filter jobs matching a workflow")
    parser_ls.add_argument('--verbose', help="Detailed BalsamJob info", action='store_true')
    parser_ls.add_argument('--tree', action='store_true', help="show DAG in tree format")
    # -----------------------------------------------------------


    # MODIFY
    # ------
    parser_modify = subparsers.add_parser('modify', help="alter job or application")
    parser_modify.set_defaults(func=modify)
    parser_modify.add_argument('id', help="substring of job or app ID to match")
    parser_modify.add_argument('attr', help="attribute of job or app to modify")
    parser_modify.add_argument('value', help="modify attr to this value")
    parser_modify.add_argument('--type', choices=['jobs', 'apps'], default='jobs', help="modify a job or AppDef")
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
    parser_mkchild.add_argument('--ranks-per-node',
                            type=int, required=True)
    
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
    parser_launcher = subparsers.add_parser('launcher', help="Start a local instance of the balsam launcher")
    parser_launcher = config_launcher_subparser(parser_launcher)
    parser_launcher.set_defaults(func=launcher)
    # -----------------

    # SUBMIT-LAUNCH
    # -------------
    parser_submitlaunch = subparsers.add_parser('submit-launch', help="Submit a launcher job to the batch queue")
    parser_submitlaunch.add_argument('-n', '--nodes', type=int, required=True)
    parser_submitlaunch.add_argument('-t', '--time-minutes', type=int, required=True)
    parser_submitlaunch.add_argument('-q', '--queue', type=str, required=True)
    parser_submitlaunch.add_argument('-A', '--project', type=str, required=True)
    parser_submitlaunch.add_argument('--job-mode', type=str, choices=['serial', 'mpi'], required=True)
    parser_submitlaunch.add_argument('--wf-filter', type=str, default='')
    parser_submitlaunch.set_defaults(func=submitlaunch)
    
    # INIT
    # --------
    parser_init = subparsers.add_parser('init', help="Create new balsam DB")
    parser_init.add_argument('path', help="Path to Balsam DB directory")
    parser_init.set_defaults(func=init)
    # -----------------


    # SERVICE
    # -------
    parser_service = subparsers.add_parser('service', help="Start Balsam auto-scheduling service")
    parser_service.set_defaults(func=service)
    # -------------------------

    # DUMMIES
    # ---------
    parser_dummy = subparsers.add_parser('make_dummies')
    parser_dummy.add_argument('num', type=int)
    parser_dummy.set_defaults(func=make_dummies)
    
    # WHICH
    # ---------
    parser_which = subparsers.add_parser('which', help="Get info on current/available DBs")
    parser_which.add_argument('--list', action='store_true', help="list cached DB paths")
    parser_which.add_argument('--name', help='Look up DB path from a partial name')
    parser_which.set_defaults(func=which)
    
    # LOG
    # ---------
    parser_log = subparsers.add_parser('log', help="Quick view of Balsam log files")
    parser_log.set_defaults(func=log)
    
    # SERVER
    # ---------
    parser_server = subparsers.add_parser('server', help="Control Balsam server at BALSAM_DB_PATH")
    group = parser_server.add_mutually_exclusive_group(required=True)
    group.add_argument('--connect', action='store_true', help="connect to existing or start new server")
    group.add_argument('--reset', action='store_true', help="stop and start server")
    group.add_argument('--list-active-connections', action='store_true', help="see how many clients have active connection")
    group.add_argument('--list-users', action='store_true', help="list authorized Balsam users")
    group.add_argument('--add-user', type=str, help="add an authorized Balsam user")
    group.add_argument('--drop-user', type=str, help="drop a user from the DB")
    group.set_defaults(func=server)

    return parser


if __name__ == "__main__":
    main()
