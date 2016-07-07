

''' Scheduler Exceptions '''

class SubmitNonZeroReturnCode(Exception): pass
class SubmitSubprocessFailed(Exception): pass
class JobSubmitFailed(Exception): pass
class JobSubmissionDisabled(Exception): pass
class JobStatusFailed(Exception): pass
class JobStatusNotFound(Exception): pass