from balsam.site import ApplicationDefinition

class hello(ApplicationDefinition):
    """
    Application description
    """
    environment_variables = {}
    command_template = 'echo hello world'
    parameters = {}
    transfers = {}

    def preprocess(self):
        self.job.state = "PREPROCESSED"
        print('biden is now president')

    def postprocess(self):
        self.job.state = "POSTPROCESSED"

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"
