''' Scheduler Exceptions '''

class SchedulerException(Exception): pass
class SubmitNonZeroReturnCode(SchedulerException): pass
class SubmitSubprocessFailed(SchedulerException): pass
class JobSubmitFailed(SchedulerException): pass
class JobSubmissionDisabled(SchedulerException): pass
class JobStatusFailed(SchedulerException): pass
class StatusNonZeroReturnCode(SchedulerException): pass
class NoQStatInformation(SchedulerException): pass
