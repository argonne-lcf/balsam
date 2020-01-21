from .base_model import BalsamModel, PydanticModel, validator
import jinja2, jinja2.meta
import shlex
import subprocess

class App(BalsamModel):

    class DataClass(PydanticModel):
        command_template: str = 'echo Hello, {{name}}!'

        @validator('command_template')
        def exec_exists(cls, v):
            split_cmd = v.strip().split()
            cmd = ' '.join(split_cmd)
            exe = split_cmd[0]
            if not cmd[0].isalnum():
    
    def __init__(self):
        self.command_template = ' '.join(self.command_template.strip().split())
        ctx = jinja2.Environment().parse(self.command_template)
        self.parameters = jinja2.meta.find_undeclared_variables(ctx)

    def render_command(self, arg_dict):
        """
        Args:
            - arg_dict: value for each required parameter
        Returns:
            - str: shell command with safely-escaped parameters
            Use shlex.split() to split the result into args list.
            Do *NOT* use string.join on the split list: unsafe!
        """
        diff = self.parameters.difference(arg_dict.keys())
        if diff:
            raise ValueError(f'Missing required args: {diff}')

        sanitized_args = {
            key: shlex.quote(str(arg_dict[key]))
            for key in self.parameters
        }
        return jinja2.Template(self.command_template).render(sanitized_args)

    def preprocess(self):
        pass
    
    def postprocess(self):
        pass

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.rerun()
    
    def handle_error(self):
        self.job.fail()


if __name__ == "__main__":
    app = App()
    cmd_str = app.render_command({'name': ";'world", 'baz':''})
    print("cmd_str:", cmd_str)
    subprocess.run(cmd_str, shell=True, executable='/bin/bash')
