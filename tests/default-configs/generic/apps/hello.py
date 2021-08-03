from balsam.site import ApplicationDefinition


class Hello(ApplicationDefinition):
    """
    Hello world app demo
    """

    command_template = 'echo "Hello, {{ name }}!"'

    def preprocess(self):
        parents = self.job.parent_query()
        for parent in parents:
            print("Parent workdir:", parent.workdir)
        self.job.state = "PREPROCESSED"
