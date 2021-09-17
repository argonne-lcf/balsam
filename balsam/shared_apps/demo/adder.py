from balsam.api import ApplicationDefinition


class Adder(ApplicationDefinition):
    site = 0

    def run(self, x: int, y: int) -> int:  # type: ignore
        return x + y
