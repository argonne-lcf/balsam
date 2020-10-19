import importlib
import inspect
import pathlib
import shlex
import jinja2
import jinja2.meta
import logging

logger = logging.getLogger(__name__)


class ApplicationDefinitionMeta(type):

    _app_registry = []

    def __new__(mcls, name, bases, attrs):
        super_new = super().__new__
        cls = super_new(mcls, name, bases, attrs)
        if not bases:
            return cls

        if "parameters" not in attrs:
            raise AttributeError(
                "Must set `parameters` dict on the ApplicationDefinition class."
            )
        if "command_template" not in attrs or not isinstance(
            attrs["command_template"], str
        ):
            raise AttributeError(
                "ApplicationDefiniton must define a `command_template` string."
            )

        cls.command_template = " ".join(cls.command_template.strip().split())
        ctx = jinja2.Environment().parse(cls.command_template)

        detected_params = jinja2.meta.find_undeclared_variables(ctx)
        cls_params = set(cls.parameters.keys())

        extraneous = cls_params.difference(detected_params)
        if extraneous:
            raise AttributeError(
                f"App {name} has extraneous `parameters` not referenced "
                f"in the command template: {extraneous}"
            )
        for param in detected_params.difference(cls_params):
            cls.parameters[param] = {
                "required": True,
                "default": None,
                "help": "",
            }

        class_path = f"{inspect.getmodule(cls).__name__}.{name}"
        ApplicationDefinitionMeta._app_registry[class_path] = cls
        return cls


class ApplicationDefinition(metaclass=ApplicationDefinitionMeta):
    class ModuleLoadError(Exception):
        pass

    _loaded_apps = {}
    environment_variables = {}
    command_template = ""
    parameters = {}
    stage_ins = {}
    stage_outs = {}

    def __init__(self, job):
        self.job = job

    @property
    def arg_str(self):
        return self._render_command(self.job.parameters)

    @property
    def arg_list(self):
        return shlex.split(self.arg_str)

    def _render_command(self, arg_dict):
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
        self.job.state = "PREPROCESSED"

    def postprocess(self):
        self.job.state = "POSTPROCESSED"

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"

    @classmethod
    def load_app_class(cls, app_path, class_name):
        """
        Load the ApplicationDefinition subclass located in directory app_path
        """
        key = (app_path, class_name)
        if key in cls._loaded_apps:
            return cls._loaded_apps[key]

        app_path = pathlib.Path(app_path)
        if not app_path.is_dir():
            raise FileNotFoundError(f"No such directory {app_path}")

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

        fpath = app_path.joinpath(filename + ".py")
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

        cls._loaded_apps[key] = app_class
        return app_class

    @classmethod
    def as_dict(cls):
        return dict(
            description=cls.__doc__,
            parameters=cls.parameters,
            stage_ins=cls.stage_ins,
            stage_outs=cls.stage_outs,
        )


app_template = '''
from balsam import ApplicationDefinition

class {{cls_name}}(ApplicationDefinition):
    """
    {{description}}
    """
    environment_variables = {}
    command_template = '{{command_template}}'
    parameters = {}
    stage_ins = {}
    stage_outs = {}

    def preprocess(self):
        pass

    def postprocess(self):
        pass

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        pass

'''.lstrip()

app_template = jinja2.Template(app_template)
