from itertools import combinations
from .exceptions import *
from .exceptions import InvalidStateError


STATES = '''
CREATED
AWAITING_PARENTS
READY

STAGED_IN
PREPROCESSED

RUNNING
RUN_DONE

POSTPROCESSED
JOB_FINISHED

RUN_TIMEOUT
RUN_ERROR
RESTART_READY

FAILED
PARENT_FAILED
USER_KILLED
'''.split()

ACTIVE_STATES = '''
RUNNING
'''.split()

PROCESSABLE_STATES = '''
CREATED
AWAITING_PARENTS
READY
STAGED_IN
RUN_DONE
POSTPROCESSED
RUN_TIMEOUT
RUN_ERROR
'''.split()

RUNNABLE_STATES = '''
PREPROCESSED
RESTART_READY
'''.split()

END_STATES = '''
JOB_FINISHED
PARENT_FAILED
FAILED
USER_KILLED
'''.split()

def validate_state(value):
    if value not in STATES:
        raise InvalidStateError(
        f"{value} is not a valid state in balsam.models"
    )

def assert_disjoint():
    groups = [ACTIVE_STATES, PROCESSABLE_STATES, RUNNABLE_STATES, END_STATES]
    joined = [state for g in groups for state in g]
    assert len(joined) == len(set(joined)) == len(STATES)
    assert set(joined) == set(STATES) 
    for g1,g2 in combinations(groups, 2):
        s1,s2 = set(g1), set(g2)
        assert s1.intersection(s2) == set()
assert_disjoint()
