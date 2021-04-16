from balsam.site import ApplicationDefinition


class Hello(ApplicationDefinition):
    """
    Application description
    """

    environment_variables = {}
    command_template = "echo hello {{ name }}"
    parameters = {}
    transfers = {}

    def preprocess(self):
        self.job.state = "PREPROCESSED"
        print("preprocessing  demo")

    def postprocess(self):
        self.job.state = "POSTPROCESSED"
        print("postprocessing demo")

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"
