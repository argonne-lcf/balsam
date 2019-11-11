from .fields import Field
from .base_model import Model

class Job(Model):

    name = Field()
    workflow = Field()
