import importlib.util
from pathlib import Path
import shlex
from typing import Tuple
import jinja2
import jinja2.meta
import logging

logger = logging.getLogger(__name__)


def split_class_path(class_path: str) -> Tuple[str, str]:
    """
    'my_module.ClassName' --> ('my_module', 'ClassName')
    """
    filename, *class_name = class_path.split(".")
    class_name = ".".join(class_name)
    if not filename:
        raise ValueError(
            f"{class_path} must refer to a Python class with the form Module.Class"
        )
    if not class_name or "." in class_name:
        raise ValueError(
            f"{class_path} must refer to a Python class with the form Module.Class"
        )
    return filename, class_name


def load_module(fpath):
    if not Path(fpath).is_file():
        raise FileNotFoundError(f"Could not find App definition file {fpath}")

    fpath = str(fpath)
    spec = importlib.util.spec_from_file_location(fpath, fpath)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        logger.exception(
            f"Failed to load {fpath} because of an exception in the module:\n{exc}"
        )
        raise
    return module


def is_appdef(attr):
    return (
        isinstance(attr, type)
        and issubclass(attr, ApplicationDefinition)
        and attr.__name__ != "ApplicationDefinition"
    )


def find_app_classes(module):
    app_classes = []
    for obj_name in dir(module):
        attr = getattr(module, obj_name)
        if is_appdef(attr):
            app_classes.append(attr)
    return app_classes


class ApplicationDefinitionMeta(type):
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
        return cls


class ApplicationDefinition(metaclass=ApplicationDefinitionMeta):
    class ModuleLoadError(Exception):
        pass

    _loaded_apps: dict = {}
    environment_variables: dict = {}
    command_template: str = ""
    parameters: dict = {}
    transfers: dict = {}

    def __init__(self, job):
        self.job = job

    @property
    def arg_str(self) -> str:
        return self._render_command(self.job.parameters)

    @property
    def arg_list(self) -> list:
        return shlex.split(self.arg_str)

    def _render_command(self, arg_dict: dict) -> str:
        """
        Args:
            - arg_dict: value for each required parameter
        Returns:
            - str: shell command with safely-escaped parameters
            Use shlex.split() to split the result into args list.
            Do *NOT* use string.join on the split list: unsafe!
        """
        diff = set(self.parameters.keys()).difference(arg_dict.keys())
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
        return []

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"

    @classmethod
    def load_app_class(cls, apps_dir, class_path):
        """
        Load the ApplicationDefinition subclass located in directory apps_dir
        """
        key = (str(apps_dir), str(class_path))
        if key in cls._loaded_apps:
            return cls._loaded_apps[key]

        apps_dir = Path(apps_dir)

        filename, class_name = split_class_path(class_path)
        fpath = apps_dir.joinpath(filename + ".py")
        module = load_module(fpath)

        app_class = getattr(module, class_name, None)
        if app_class is None:
            raise AttributeError(
                f"Loaded module at {fpath}, but it does not contain the class {class_name}"
            )
        if not issubclass(app_class, cls):
            raise TypeError(f"{class_path} must subclass {cls.__name__}")

        cls._loaded_apps[key] = app_class
        return app_class

    @classmethod
    def as_dict(cls):
        return dict(
            description=cls.__doc__, parameters=cls.parameters, transfers=cls.transfers,
        )


app_template = '''
from balsam.site import ApplicationDefinition

class {{cls_name}}(ApplicationDefinition):
    """
    {{description}}
    """
    environment_variables = {}
    command_template = '{{command_template}}'
    parameters = {}
    transfers = {}

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

'''.lstrip()

app_template = jinja2.Template(app_template)
