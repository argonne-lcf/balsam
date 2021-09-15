from balsam.api import ApplicationDefinition


class Hello(ApplicationDefinition):
    site = 0
    command_template = "echo hello {{ name }}"
