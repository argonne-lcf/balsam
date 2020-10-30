from balsam.site import ApplicationDefinition


class Hello(ApplicationDefinition):
    """
    Hello world app demo
    """

    command_template = 'echo "Hello, {{ name }}!"'
    parameters = {}
