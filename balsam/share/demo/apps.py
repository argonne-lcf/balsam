from balsam.api import ApplicationDefinition


class Hello(ApplicationDefinition):
    site = 0
    command_template = "echo hello {{ name }}"


class Adder(ApplicationDefinition):
    site = 0

    def run(self, x: int, y: int) -> int:  # type: ignore
        return x + y
