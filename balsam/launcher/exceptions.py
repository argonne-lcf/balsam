class BalsamLauncherError(Exception): pass

class BalsamRunnerError(Exception): pass
class ExceededMaxRunners(BalsamRunnerError): pass
class NoAvailableWorkers(BalsamRunnerError): pass

class BalsamTransitionError(Exception): pass
class TransitionNotFoundError(BalsamTransitionError, ValueError): pass

class MPIEnsembleError(Exception): pass
