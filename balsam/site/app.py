import importlib.util
import logging
import os
import re
import shlex
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Type, Union, cast

import click
import jinja2
import jinja2.meta

from balsam.schemas import JobState

if TYPE_CHECKING:
    from balsam._api.models import App, Job
    from balsam.client import RESTClient
    from balsam.config import SiteConfig

ModTimeDict = Dict[str, float]
AppClassDict = Dict[str, List[Type["ApplicationDefinition"]]]

logger = logging.getLogger(__name__)


def split_class_path(class_path: str) -> Tuple[str, str]:
    """
    'my_module.ClassName' --> ('my_module', 'ClassName')
    """
    filename, *class_name_tup = class_path.split(".")
    class_name = ".".join(class_name_tup)
    if not filename:
        raise ValueError(f"{class_path} must refer to a Python class with the form Module.Class")
    if not class_name or "." in class_name:
        raise ValueError(f"{class_path} must refer to a Python class with the form Module.Class")
    return filename, class_name


def load_module(fpath: Union[str, Path]) -> ModuleType:
    if not Path(fpath).is_file():
        raise FileNotFoundError(f"Could not find App definition file {fpath}")

    fpath = str(fpath)
    spec = importlib.util.spec_from_file_location(fpath, fpath)
    if spec is None:
        raise ImportError(f"Failed to load module spec; please double check {fpath}")
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise ImportError(f"Failed to load {fpath}: spec has no loader")
    try:
        cast(Any, spec.loader).exec_module(module)
    except Exception as exc:
        logger.exception(f"Failed to load {fpath} because of an exception in the module:\n{exc}")
        raise
    return module


def is_appdef(attr: type) -> bool:
    return (
        isinstance(attr, type)
        and issubclass(attr, ApplicationDefinition)
        and attr.__name__ != "ApplicationDefinition"
    )


def find_app_classes(module: ModuleType) -> List[Type["ApplicationDefinition"]]:
    app_classes = []
    for obj_name in dir(module):
        attr = getattr(module, obj_name)
        if is_appdef(attr):
            app_classes.append(attr)
    return app_classes


def sync_app(
    client: "RESTClient",
    app_class: Type["ApplicationDefinition"],
    class_path: str,
    mtime: float,
    registered_app: Optional["App"],
    site_id: int,
) -> None:
    # App not in DB: create it
    if registered_app is None:
        app = client.App.objects.create(
            site_id=site_id,
            class_path=class_path,
            last_modified=mtime,
            **app_class.as_dict(),
        )
        click.echo(f"CREATED    {class_path} (app_id={app.id})")
    # App out of date; update it:
    elif registered_app.last_modified is None or registered_app.last_modified < mtime:
        for k, v in app_class.as_dict().items():
            setattr(registered_app, k, v)
        registered_app.last_modified = mtime
        registered_app.save()
        click.echo(f"UPDATED         {class_path} (app_id={registered_app.id})")
    else:
        click.echo(f"UP-TO-DATE      {class_path} (app_id={registered_app.id})")
    # Otherwise, app is up to date :)
    return


def app_deletion_prompt(client: "RESTClient", app: "App") -> None:
    job_count = client.Job.objects.filter(app_id=app.id).count()
    click.echo(f"DELETED/RENAMED {app.class_path} (app_id={app.id})")
    click.echo("   --> You either renamed this ApplicationDefinition or deleted it.")
    click.echo(f"   --> There are {job_count} Jobs associated with this App")
    delete = click.confirm(f"  --> Do you wish to unregister this App (this will ERASE {job_count} jobs!)")
    if delete:
        app.delete()
        click.echo("  --> Deleted.")
    else:
        click.echo("  --> App not deleted. If you meant to rename it, please update the class_path in the API.")


def sync_apps(site_config: "SiteConfig") -> None:
    client = site_config.client
    registered_apps = list(client.App.objects.filter(site_id=site_config.site_id))
    app_classes, mtimes = load_apps_from_dir(site_config.apps_path)

    for module_name, app_class_list in app_classes.items():
        for app_class in app_class_list:
            class_path = f"{module_name}.{app_class.__name__}"
            registered_app = next((a for a in registered_apps if a.class_path == class_path), None)
            sync_app(
                client,
                app_class,
                class_path,
                mtimes[module_name],
                registered_app,
                site_config.site_id,
            )
            if registered_app is not None:
                registered_apps.remove(registered_app)

    # Remaining registered_apps are no longer in the apps_path
    # They could have been deleted or renamed
    for app in registered_apps:
        app_deletion_prompt(client, app)


def load_apps_from_dir(apps_path: Union[str, Path]) -> Tuple[AppClassDict, ModTimeDict]:
    """
    Fetch all ApplicationDefinitions and their local modification times
    Returns two dicts keyed by module name
    """
    app_files = list(Path(apps_path).glob("*.py"))
    mtimes = {}
    app_classes = {}
    for fname in app_files:
        module_name = fname.with_suffix("").name
        mtimes[module_name] = fname.stat().st_mtime
        module = load_module(fname)
        app_classes[module_name] = find_app_classes(module)
    return app_classes, mtimes


class ApplicationDefinitionMeta(type):
    _loaded_apps: Dict[Tuple[str, str], Type["ApplicationDefinition"]]
    _param_pattern = re.compile(r"{{(.*?)}}")
    environment_variables: Dict[str, str]
    command_template: str
    parameters: Dict[str, Any]
    transfers: Dict[str, Any]
    cleanup_files: List[str]
    _default_params: Dict[str, str]

    def __new__(mcls, name: str, bases: Tuple[Any, ...], attrs: Dict[str, Any]) -> "ApplicationDefinitionMeta":
        super_new = super().__new__
        if "parameters" not in attrs:
            attrs["parameters"] = {}
        cls = super_new(mcls, name, bases, attrs)
        if not bases:
            return cls

        if "command_template" not in attrs or not isinstance(attrs["command_template"], str):
            raise AttributeError(f"ApplicationDefiniton {name} must define a `command_template` string.")

        # Validate command template
        cls.command_template = cls.command_template.strip()
        for param in re.findall(mcls._param_pattern, cls.command_template):
            if not param.strip().isidentifier():
                raise AttributeError(
                    f"ApplicationDefinition {name} parameter '{param.strip()}' is not a valid Python identifier."
                )

        # Detect parameters via Jinja
        ctx = jinja2.Environment().parse(cls.command_template)
        detected_params: Set[str] = jinja2.meta.find_undeclared_variables(ctx)

        # Validate parameters dict
        for param_name, param in cls.parameters.items():
            if not param_name.isidentifier():
                raise AttributeError(f"{name} dictionary parameter '{param_name}' is not a valid Python identifier.")
            if "required" not in param:
                raise AttributeError(
                    f"Application Definition {name} parameter {param_name} must set 'required' key as True/False"
                )
            if not param["required"] and param.get("default") is None:
                raise AttributeError(
                    f"ApplicationDefinition {name} optional parameter {param_name} must set a 'default' str value"
                )

        cls_params = set(cls.parameters.keys())
        extraneous = cls_params.difference(detected_params)
        if extraneous:
            raise AttributeError(
                f"App {name} has extraneous keys "
                f"in the `parameters` dictionary: {extraneous}"
                f"\nparameters dict: {cls_params}"
                f"\nDetected parameters in command_template: {detected_params}"
            )
        for param in detected_params.difference(cls_params):
            cls.parameters[param] = {
                "required": True,
                "default": None,
                "help": "",
            }
        cls._default_params = {k: v["default"] for k, v in cls.parameters.items() if not v["required"]}
        return cls


class ApplicationDefinition(metaclass=ApplicationDefinitionMeta):
    class ModuleLoadError(Exception):
        pass

    _loaded_apps: Dict[Tuple[str, str], Type["ApplicationDefinition"]] = {}
    environment_variables: Dict[str, str] = {}
    command_template: str = ""
    parameters: Dict[str, Any] = {}
    transfers: Dict[str, Any] = {}
    cleanup_files: List[str] = []
    _default_params: Dict[str, str]

    def __init__(self, job: "Job") -> None:
        self.job = job

    def get_arg_str(self) -> str:
        return self._render_command({**self._default_params, **self.job.parameters})

    def get_environ_vars(self) -> Dict[str, str]:
        envs = os.environ.copy()
        envs.update(self.environment_variables)
        envs["BALSAM_JOB_ID"] = str(self.job.id)
        if self.job.threads_per_rank > 1:
            envs["OMP_NUM_THREADS"] = str(self.job.threads_per_rank)
        return envs

    def _render_command(self, arg_dict: Dict[str, str]) -> str:
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
            raise ValueError(f"Missing required args: {diff} (only got: {arg_dict})")

        sanitized_args = {key: shlex.quote(str(arg_dict[key])) for key in self.parameters}
        return jinja2.Template(self.command_template).render(sanitized_args)

    def preprocess(self) -> None:
        self.job.state = JobState.preprocessed

    def postprocess(self) -> None:
        self.job.state = JobState.postprocessed

    def shell_preamble(self) -> Union[str, List[str]]:
        return []

    def handle_timeout(self) -> None:
        self.job.state = JobState.restart_ready

    def handle_error(self) -> None:
        self.job.state = JobState.failed

    @classmethod
    def load_app_class(cls, apps_dir: Union[Path, str], class_path: str) -> Type["ApplicationDefinition"]:
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
            raise AttributeError(f"Loaded module at {fpath}, but it does not contain the class {class_name}")
        if not issubclass(app_class, cls):
            raise TypeError(f"{class_path} must subclass {cls.__name__}")

        cls._loaded_apps[key] = app_class
        return cast(Type[ApplicationDefinition], app_class)

    @classmethod
    def as_dict(cls) -> Dict[str, Any]:
        return dict(
            description=(cls.__doc__ or "").strip(),
            parameters=cls.parameters,
            transfers=cls.transfers,
        )


app_template = jinja2.Template(
    '''
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
)
