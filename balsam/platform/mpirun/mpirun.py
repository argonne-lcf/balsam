import shlex

class MPIRun(object):

    launch_command = 'mpiexec'

    def __init__(
        self,
        app_args,
        node_ids,
        num_ranks=1,
        ranks_per_node=1,
        threads_per_rank=1,
        threads_per_core=1, gpus_per_rank=0,
        cpu_list=[],
        gpu_list=[],
        env={},
        cpu_affinity='',
    ):
        if isinstance(app_args, str):
            self.app_args = shlex.split(app_args)
        elif isinstance(app_args, list):
            self.app_args = app_args
        else:
            raise TypeError(f'Expected str or list app_args; got {type(app_args)}')

        assert isinstance(node_ids, (list, tuple))
        self.node_ids = node_ids

        self.num_ranks = int(num_ranks)
        self.ranks_per_node = int(ranks_per_node)
        self.threads_per_rank = int(threads_per_rank)
        self.threads_per_core = int(threads_per_core)
        self.gpus_per_rank = int(gpus_per_rank)
        self.cpu_list = cpu_list
        self.gpu_list = gpu_list
        self.cpu_affinity = cpu_affinity
        self.env = env
    
    def __str__(self):
        return ' '.join(self.render_args())

    def __iter__(self):
        return iter(self.render_args())

    def __repr__(self):
        param_str = ', '.join(f'{k}={v}' for k,v in self.__dict__.items())
        return f'{self.__class__.__name}({param_str})'

    def render_args(self):
        launch_args = [str(a) for a in self.get_launch_args()]
        return [self.launch_command] + launch_args + self.app_args

    def get_launch_args(self):
        return []

class DirectRun(MPIRun):
    def render_args(self):
        return self.app_args
