
class JobState:
   def __init__(self,state_name,failed_state=None,transition_function=None):
      self.name                  = state_name
      self.failed_state          = failed_state
      self.transition_function   = transition_function
   def __eq__(self,other):
      return True if self.name == other.name else False
   def __ne__(self,other):
      return True if self.name != other.name else False