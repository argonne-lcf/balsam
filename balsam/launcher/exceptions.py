class BalsamLauncherError(Exception): pass

class BalsamRunnerError(Exception): pass
class ExceededMaxConcurrentRunners(BalsamRunnerException): pass
class NoAvailableWorkers(BalsamRunnerException): pass

class BalsamTransitionError(Exception): pass
class TransitionNotFoundError(BalsamTransitionException, ValueError): pass

class MPIEnsembleError(Exception): pass
