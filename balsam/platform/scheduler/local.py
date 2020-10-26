from .scheduler import SubprocessSchedulerInterface


class LocalProcessScheduler(SubprocessSchedulerInterface):
    def _render_submit_args(
        self, script_path, project, queue, num_nodes, wall_time_min, **kwargs
    ):
        pass
