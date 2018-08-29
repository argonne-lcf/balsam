import os
import sys
import stat
from socket import gethostname
import time

from jinja2 import Template, Environment, FileSystemLoader

from django.conf import settings
from balsam.service.schedulers.exceptions import *
import logging
logger = logging.getLogger(__name__)

class Scheduler:
    SCHEDULER_VARIABLES = {}
    JOBSTATUS_VARIABLES = {}

    def __init__(self):
        logger.debug(f"Using scheduler class {self.__class__}")
        self.JobEnv = JobEnvironment(self)
        self._template = self._load_submit_template()

    def submit(self, qlaunch):
        config = self._qlaunch_to_dict(qlaunch)
        jobscript = self._template.render(config)
        path = self._write_submit_script(jobscript, qlaunch)
        submit_cmd = self._make_submit_cmd(path, qlaunch)
        p = subprocess.run(submit_cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, encoding='utf-8')
        if p.returncode != 0: raise SubmitNonZeroReturnCode
        submitinfo = self._parse_submit_output(p.stdout)
        qlaunch.scheduler_id = submitinfo['scheduler_id']
        qlaunch.state = 'queued'
        qlaunch.save()
        logger.debug(f'Qlaunch {qlaunch.pk} submitted:\n{qlaunch}')

    def _write_submit_script(self, jobscript, qlaunch):
        pk = qlaunch.pk
        fname = f'qlaunch{pk}.sh'
        path = os.path.join(settings.SERVICE_PATH, fname)
        assert not os.path.exists(path)
        with open(path, 'w') as fp:
            fp.write(jobscript)
        st = os.stat(path)
        os.chmod(path, st.st_mode | stat.S_IEXEC)
        return path

    def _load_submit_template(self):
        schedClass = self.__class__.__name__
        hostType = self.JobEnv.host_type
        
        here = os.path.dirname(os.path.abspath(__file__))
        templ_path = os.path.join(here, 'templates')
        env = Environment(loader=FileSystemLoader(templ_path))
        fname = f'{hostType}.{schedClass}.tmpl'.lower()
        return env.get_template(fname)

    def _qlaunch_to_dict(self, qlaunch):
        project = settings.DEFAULT_PROJECT
        if qlaunch.wf_filter:
            wf_filter = f'wf_filter={qlaunch.wf_filter}'
        else:
            wf_filter = 'consume-all'
        conf = dict(project=project,
                    queue=qlaunch.queue,
                    nodes=qlaunch.nodes,
                    time_minutes=qlaunch.wall_minutes,
                    job_mode=qlaunch.job_mode,
                    wf_filter=wf_filter,
                    serial_jobs_per_node=qlaunch.serial_jobs_per_node)
        balsam_env = get_balsam_env()
        conf.update(balsam_env)
        return conf

class JobEnvironment:
    RECOGNIZED_HOSTS = {
        'BGQ'    : 'vesta cetus mira'.split(),
        'THETA'   : 'theta'.split(),
        'COOLEY' : 'cooley cc'.split()
    }

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.pid = os.getpid()
        self.hostname = gethostname()
        self.host_type = 'DEFAULT'
        
        self.current_scheduler_id = None
        self.num_workers = None
        self.workers_str = None
        self.workers_file = None
        self.remaining_seconds = None
        self.get_env()
        self.remaining_time_seconds()

        for host_type, known_names in self.RECOGNIZED_HOSTS.items():
            if any(self.hostname.find(name) >= 0 for name in known_names):
                self.host_type = host_type
        logger.debug(f"Recognized host_type: {self.host_type}")

    def get_env(self):
        '''Check for environment variables (e.g. COBALT_JOBID) indicating 
        currently inside a scheduled job'''
        for generic_name, specific_var in self.scheduler.SCHEDULER_VARIABLES.items():
            value = os.environ.get(specific_var, None)
            if value is not None and value.startswith('num'): value = int(value)
            setattr(self, generic_name, value)

        if self.current_scheduler_id:
            logger.debug(f"Detected scheduler ID {self.current_scheduler_id}")
        else:
            logger.debug(f"Did not detect a {self.scheduler} ID")

    def _read_remaining_seconds(self):
        if self.current_scheduler_id is None:
            return float("inf")
        try:
            info = self.scheduler.get_status(self.current_scheduler_id)
        except JobStatusFailed:
            return float("inf")
        else:
            return info['time_remaining_sec']

    def remaining_time_seconds(self):
        if self.remaining_seconds is None:
            self.remaining_seconds = self._read_remaining_seconds()
            self.last_check_seconds = time.time()
        now = time.time()
        elapsed_time = now - self.last_check_seconds
        self.remaining_seconds -= elapsed_time
        self.last_check_seconds = now
        return self.remaining_seconds

def get_balsam_env():
    conda_env = os.environ.get('CONDA_PREFIX')
    if conda_env:
        activate_path = os.path.join(conda_env, 'bin', 'activate')
        conda_path = conda_env
        python_path = None
    else:
        activate_path = None
        conda_path = None
        python_path = os.path.dirname(sys.executable)

    balsam_lib = os.path.dirname(find_spec('balsam').origin)
    p = subprocess.run('which balsam', shell=True, stdout=subprocess.PIPE, encoding='utf-8')
    balsam_bin = os.path.dirname(p.stdout)
    site_top = os.path.dirname(os.path.dirname(find_spec('django').origin))
    balsam_db_path = os.environ['BALSAM_DB_PATH']
    return dict(conda_path=conda_path,
                activate_path=activate_path,
                python_path=python_path,
                balsam_lib=balsam_lib,
                balsam_bin=balsam_bin,
                site_top=site_top,
                balsam_db_path=balsam_db_path)
