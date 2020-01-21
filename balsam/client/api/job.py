from .base_model import BalsamModel, PydanticModel

class Job(BalsamModel):

    class DataClass(PydanticModel):
        name: str
        workflow: str
        num_nodes: int
        cpu_affinity = 'depth'

    def __init__(self, name, workflow, num_nodes, cpu_affinity='depth'):
        super().__init__(
            name=name,
            workflow=workflow,
            num_nodes=num_nodes,
            cpu_affinity=cpu_affinity
        )
