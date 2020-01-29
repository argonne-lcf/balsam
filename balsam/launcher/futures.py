from collections import namedtuple, defaultdict
import time
from balsam import setup
setup()
from balsam.core.models import BalsamJob
from balsam.core.models import END_STATES
from balsam.launcher import dag

class TaskFailed(Exception): pass
WaitResult = namedtuple('WaitResult', ['active', 'done', 'failed', 'cancelled'])

def _timer(timeout):
    if timeout is None: 
        return lambda : True
    else:
        timeout = max(float(timeout), 0.01)
        start = time.time()
        return lambda : (time.time()-start) < timeout

def _to_state(state):
    if state not in END_STATES:
        return 'active'
    elif state == 'JOB_FINISHED':
        return 'done'
    elif state == 'FAILED':
        return 'failed'
    else:
        assert state == 'USER_KILLED'
        return 'cancelled'

def wait(futures, timeout=None, return_when='ANY_COMPLETED', poll_period=1.0):
    assert return_when.strip() in ['ANY_COMPLETED', 'ALL_COMPLETED']
    waitall = bool(return_when.strip() == 'ALL_COMPLETED')

    time_isLeft = _timer(timeout)
    futures = {future._job.pk:future for future in futures}
    finished_pks = []

    if waitall: can_exit = lambda : len(finished_pks) == len(futures)
    else: can_exit = lambda : len(finished_pks) > 0

    while time_isLeft():
        pks = [pk for pk in futures if pk not in finished_pks]
        jobs = BalsamJob.objects.filter(pk__in=pks)
        for j in jobs:
            futures[j.pk]._job = j
            futures[j.pk]._state = _to_state(j.state)
            if j.state in END_STATES: finished_pks.append(j.pk)
        if can_exit(): break
        else: time.sleep(poll_period)

    if not can_exit():
        raise TimeoutError(f'{timeout} sec timeout expired while '
        f'waiting on {len(futures)} tasks until {return_when}')

    results = defaultdict(list)
    for f in futures.values(): results[f._state].append(f)
    return WaitResult(
        active=results['active'],
        done=results['done'],
        failed=results['failed'],
        cancelled=results['cancelled']
    )

class FutureTask:
    def __init__(self, balsam_job, done_callback, fail_callback=None, *args, **kwargs):
        '''Create a FutureTask from a BalsamJob object and callback function.

        Args
            balsam_job: BalsamJob object to be wrapped and monitored
            done_callback: a function of one positional argument (BalsamJob) and
                optional *args, **kwargs. done_callback(balsam_job, *args,
                **kwargs) is invoked and its return value is returned upon calling
                the result() method of a FutureTask.
            fail_callback (optional): a function with the same signature as done_callback,
                which is invoked upon calling result() if the task fails. 
            *args, **kwargs: optional args and keyword args to the done and fail
            callbacks.
        '''
        assert isinstance(balsam_job, BalsamJob)
        self._job = balsam_job
        self._on_done = done_callback
        self._on_fail = fail_callback
        assert callable(self._on_done)

        self._result = None
        self._state = 'active'
        self._executed_callback = False
        self._args = args
        self._kwargs = kwargs

    def __repr__(self):
        return f'<balsam.launcher.futures.FutureTask {self._job.cute_id}: {self._state}>' 

    def _poll(self):
        self._job = BalsamJob.objects.get(pk=self._job.pk)
        self._state = _to_state(self._job.state)

    def _callback(self, success):
        callback = self._on_done if success else self._on_fail
        if not success and not callable(callback):
            raise TaskFailed(f'Task {self._job.cute_id} failed; no fail_callback was set to handle this.')
        if not self._executed_callback:
            self._result = callback(self._job, *self._args, **self._kwargs)
            self._executed_callback = True

    @property
    def done(self):
        if self._state == 'active': self._poll()
        return self._state == 'done'

    @property
    def failed(self):
        if self._state == 'active': self._poll()
        return self._state == 'failed'
    
    @property
    def cancelled(self):
        if self._state == 'active': self._poll()
        return self._state == 'cancelled'

    @property
    def active(self):
        if self._state == 'active': self._poll()
        return self._state == 'active'

    def result(self, timeout=None):
        if self._executed_callback: return self._result

        time_isLeft = _timer(timeout)

        while time_isLeft():
            if self.active: time.sleep(1)
            else: break

        if self.active:
            raise TimeoutError(f"Task {self.cute_id} not finished after {timeout} seconds")
        elif self.cancelled:
            return None
        elif self.done:
            self._callback(True)
            return self._result
        else:
            assert self.failed
            self._callback(False)
            return self._result

    def cancel(self):
        dag.kill(self._job)
        self._state = 'cancelled'
