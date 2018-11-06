class BalsamLauncherError(Exception): pass

class BalsamTransitionError(Exception): pass
class TransitionNotFoundError(BalsamTransitionError, ValueError): pass

class MPIEnsembleError(Exception): pass
