from abc import ABC, abstractmethod
import os
from pathlib import Path
import subprocess
import tempfile
import psutil
import logging

logger = logging.getLogger(__name__)


class _MockProcess:
    def cpu_affinity(self, cpus=None):
        pass


_psutil_process = psutil.Process()
try:
    _psutil_process.cpu_affinity()
except AttributeError:
    _psutil_process = _MockProcess()
    logger.info("No psutil cpu_affinity support")


class TimeoutExpired(Exception):
    pass


class AppRun(ABC):
    """
    Balsam-wide Application launch interface
    The API defines the spec of an App invocation and manages
    its lifecycle: start/poll/kill/tail_output, etc...
    """

    def __init__(
        self,
        cmdline,
        preamble,
        envs,
        cwd,
        outfile_path,
        node_spec,
        ranks_per_node,
        threads_per_rank,
        threads_per_core,
        launch_params,
        gpus_per_rank,
    ):
        self._cmdline = cmdline
        self._preamble = preamble
        self._envs = envs
        self._cwd = cwd
        self._outfile_path = outfile_path
        self._node_spec = node_spec
        self._ranks_per_node = ranks_per_node
        self._threads_per_rank = threads_per_rank
        self._threads_per_core = threads_per_core
        self._launch_params = launch_params
        self._gpus_per_rank = gpus_per_rank

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def poll(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    @abstractmethod
    def kill(self):
        pass

    @abstractmethod
    def tail_output(self, nlines=10):
        pass

    @abstractmethod
    def wait(self, timeout=None):
        pass


class SubprocessAppRun(AppRun):
    """
    Implements subprocess management for apps launched via Popen
    """

    _preamble_cache = {}

    def _build_cmdline(self):
        raise NotImplementedError

    def _build_preamble(self):
        if not self._preamble:
            return ""
        if isinstance(self._preamble, list):
            return " && ".join(self._preamble) + " && "

        if self._preamble not in self._preamble_cache:
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as fp:
                fp.write(self._preamble)
                fp.flush()
                self._preamble_cache[self._preamble] = Path(fp.name).resolve()
        return f"source {self._preamble_cache[self._preamble]} && "

    def _get_envs(self):
        envs = os.environ.copy()
        envs.update(self._envs)
        if self._gpu_ids:
            envs["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
            envs["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, self._gpu_ids))
        return envs

    def _open_outfile(self):
        return open(self._outfile_path, "wb")

    def _pre_popen(self):
        pass

    def _post_popen(self):
        pass

    def start(self):
        cmdline = self._build_preamble() + self._build_cmdline()
        self._outfile = self._open_outfile()
        envs = self._get_envs()
        self._pre_popen()

        self._process = subprocess.Popen(
            cmdline,
            shell=True,
            executable="/bin/bash",
            stdout=self._outfile,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=envs,
            cwd=self._cwd,
        )
        self._post_popen()

    def poll(self):
        returncode = self._process.poll()
        if returncode is not None:
            self._outfile.close()
        return returncode

    def terminate(self):
        return self._process.terminate()

    def kill(self):
        self._process.kill()
        self._outfile.close()

    def wait(self, timeout=None):
        try:
            retcode = self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise TimeoutExpired
        else:
            self._outfile.close()
            return retcode

    def tail_output(self, nlines=10):
        return subprocess.check_output(
            ["tail", "-n", str(nlines), self._outfile_path],
            encoding="utf-8",
        )


class LocalAppRun(SubprocessAppRun):
    def _build_cmdline(self):
        return self._cmdline

    def _pre_popen(self):
        cpu_ids = self._node_spec.cpu_ids[0]
        if cpu_ids:
            _psutil_process.cpu_affinity(cpu_ids)

    def _post_popen(self):
        _psutil_process.cpu_affinity([])
