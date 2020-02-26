import importlib
import pathlib
import shlex
import jinja2
import jinja2.meta
import logging

logger = logging.getLogger(__name__)


class ApplicationDefinition(object):
    class ModuleLoadError(Exception):
        pass

    _loaded_apps = {}

    @classmethod
    def load_app_class(cls, app_path, class_name):
        if class_name in cls._loaded_apps:
            return cls._loaded_apps[class_name]

        filename, *module_attr = class_name.split(".")
        module_attr = ".".join(module_attr)

        if not filename:
            raise ValueError(
                f"{class_name} must refer to a Python class with the form Module.Class"
            )

        if not module_attr or "." in module_attr:
            raise ValueError(
                f"{class_name} must refer to a Python class with the form Module.Class"
            )

        fpath = pathlib.Path(app_path).joinpath(filename)
        if not fpath.is_file():
            raise FileNotFoundError(f"Could not find App definition file {fpath}")

        fpath = str(fpath)
        spec = importlib.util.spec_from_file_location(fpath, fpath)
        module = importlib.util.module_from_spec(spec)

        try:
            spec.loader.exec_module(module)
        except Exception as exc:
            logger.exception(
                f"Failed to load {class_name} because of an exception in the module {fpath}:\n{exc}"
            )
            module = None

        if module is None:
            raise cls.ModuleLoadError(
                f"Failed to load {class_name} because of an exception in the module {fpath}"
            )

        app_class = getattr(module, module_attr, None)
        if app_class is None:
            raise AttributeError(
                f"Loaded module at {fpath}, but it does not contain the class {module_attr}"
            )
        if not issubclass(app_class, cls):
            raise TypeError(f"{class_name} must subclass {cls.__name__}")

        cls._loaded_apps[class_name] = app_class
        return app_class

    def exec_exists(cls, v):
        split_cmd = v.strip().split()
        cmd = " ".join(split_cmd)
        exe = split_cmd[0]
        if not exe.isalnum():
            raise RuntimeError("invalid")
        return cmd

    def __init__(self):
        self.command_template = " ".join(self.command_template.strip().split())
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
            raise ValueError(f"Missing required args: {diff}")

        sanitized_args = {
            key: shlex.quote(str(arg_dict[key])) for key in self.parameters
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
