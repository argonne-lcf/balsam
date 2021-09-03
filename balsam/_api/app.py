from functools import lru_cache
import logging
import inspect
import os
import re
import shlex
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Type, Union, cast, Callable
from enum import Enum

import jinja2
import jinja2.meta

from balsam.schemas import JobState, serialize, deserialize, get_source
from balsam._api.models import Site

if TYPE_CHECKING:
    from balsam._api.models import App, Job
    from balsam.client import RESTClient
    from balsam.config import SiteConfig

ModTimeDict = Dict[str, float]
AppClassDict = Dict[str, List[Type["ApplicationDefinition"]]]
PARAM_PATTERN = re.compile(r"{{(.*?)}}")

logger = logging.getLogger(__name__)


def is_appdef(attr: type) -> bool:
    return (
        isinstance(attr, type)
        and issubclass(attr, ApplicationDefinition)
        and attr.__name__ != "ApplicationDefinition"
    )


def is_valid_template(command_template: str) -> bool:
    for param in re.findall(PARAM_PATTERN, command_template):
        if not param.strip().isidentifier():
            return False
    return True


def validate_dict_parameter(param_name: str, param: Dict[str, str]) -> None:
    if "required" not in param:
        raise AttributeError(f"ApplicationDefinition parameter {param_name} must set 'required' key as True/False")
    if not param["required"] and param.get("default") is None:
        raise AttributeError(f"ApplicationDefinition optional parameter {param_name} must set a 'default' str value")


def detect_template_parameters(command_template: str) -> Set[str]:
    ctx = jinja2.Environment().parse(command_template)
    detected_params: Set[str] = jinja2.meta.find_undeclared_variables(ctx)
    return detected_params


def check_extraneous_parameters(detected_params: Set[str], dict_params: Set[str]) -> None:
    extraneous = dict_params.difference(detected_params)
    if extraneous:
        raise AttributeError(
            f"App has extraneous keys "
            f"in the `parameters` dictionary: {extraneous}"
            f"\nparameters dict: {dict_params}"
            f"\nDetected parameters in command_template: {detected_params}"
        )


def metadata_from_signature(param: inspect.Parameter) -> Dict[str, Any]:
    if param.annotation:
        if hasattr(param.annotation, "__name__"):
            help_text = param.annotation.__name__
        else:
            help_text = str(param.annotation)
    else:
        help_text = ""

    empty = inspect.Parameter.empty
    if param.default is empty:
        return {
            "required": True,
            "default": None,
            "help": help_text,
        }
    else:
        return {
            "required": False,
            "default": param.default,
            "help": help_text,
        }


class AppType(str, Enum):
    SHELL_CMD = "SHELL_CMD"
    PY_FUNC = "PY_FUNC"


class ApplicationDefinitionMeta(type):
    _loaded_apps: Dict[Tuple[str, str], Type["ApplicationDefinition"]]
    environment_variables: Dict[str, str]
    command_template: str
    parameters: Dict[str, Any]
    transfers: Dict[str, Any]
    cleanup_files: List[str]
    _default_params: Dict[str, str]
    _app_type: AppType
    run: Optional[Callable[..., Any]]

    @property
    def source_code(cls) -> str:
        return get_source(cls)

    def _setup_shell_app(cls) -> "ApplicationDefinitionMeta":
        cls._app_type = AppType.SHELL_CMD
        cls.command_template = cls.command_template.strip()
        if not is_valid_template(cls.command_template):
            raise AttributeError(
                f"ApplicationDefinition {cls.__name__} command_template parameters must be valid Python identifiers"
            )

        for param_name, param in cls.parameters.items():
            validate_dict_parameter(param_name, param)

        detected_params = detect_template_parameters(cls.command_template)
        dict_params = set(cls.parameters.keys())
        check_extraneous_parameters(detected_params, dict_params)

        for param in detected_params.difference(dict_params):
            cls.parameters[param] = {
                "required": True,
                "default": None,
                "help": "",
            }
        cls._default_params = {k: v["default"] for k, v in cls.parameters.items() if not v["required"]}
        return cls

    def _setup_py_app(cls) -> "ApplicationDefinitionMeta":
        assert callable(cls.run)
        cls._app_type = AppType.PY_FUNC

        signature = inspect.signature(cls.run)
        if "self" not in signature.parameters:
            raise TypeError(f"ApplicationDefinition {cls.__name__}.run{signature} is missing the `self` argument")

        for param_name, param in signature.parameters.items():
            if param_name == "self":
                continue
            cls.parameters[param_name] = metadata_from_signature(param)
        return cls

    def __new__(mcls, name: str, bases: Tuple[Any, ...], attrs: Dict[str, Any]) -> "ApplicationDefinitionMeta":
        if "site" not in attrs or not isinstance(attrs["site"], (str, int, Site)):
            raise AttributeError(
                f"ApplicationDefinition {name} must contain the `site` attribute, set to a site id, name, or Site object"
            )

        if "parameters" not in attrs:
            attrs["parameters"] = {}
        cls = super().__new__(mcls, name, bases, attrs)
        if not bases:
            return cls

        has_command_template = "command_template" in attrs and isinstance(attrs["command_template"], str)
        has_run_function = "run" in attrs and callable(attrs["run"])

        if has_command_template and has_run_function:
            raise AttributeError(
                f"ApplicationDefinition {name} must not define both `run` function and `command_template`"
            )
        elif has_command_template:
            return mcls._setup_shell_app(cls)
        elif has_run_function:
            return mcls._setup_py_app(cls)
        else:
            raise AttributeError(
                f"ApplicationDefinition {name} must define either a `run` function or a `command_template`"
            )


class ApplicationDefinition(metaclass=ApplicationDefinitionMeta):
    class ModuleLoadError(Exception):
        pass

    _loaded_apps: Dict[Tuple[str, str], Type["ApplicationDefinition"]] = {}
    environment_variables: Dict[str, str] = {}
    command_template: str
    parameters: Dict[str, Any] = {}
    transfers: Dict[str, Any] = {}
    cleanup_files: List[str] = []
    _default_params: Dict[str, str]
    run: Optional[Callable[..., Any]] = None
    site: Union[int, str, Site]
    _client: Optional[RESTClient] = None
    __app_id__: Optional[int] = None

    @staticmethod
    def _set_client(client: RESTClient) -> None:
        ApplicationDefinition._client = client

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
    def resolve_site_id(cls) -> int:
        if isinstance(cls.site, str):
            if cls._client is None:
                raise AttributeError("Client has not been set: call _set_client prior to this method")
            site = cls._client.Site.objects.get(name=cls.site)
            assert site.id is not None
            cls.site = site.id
        elif isinstance(cls.site, Site):
            if cls.site.id is None:
                raise ValueError(f"{cls.__name__}.site does not have an ID set: {cls.site}")
            cls.site = cls.site.id
        return cls.site

    @staticmethod
    def load_apps(site: Union[int, str, Site]) -> "Dict[int, Type[ApplicationDefinition]]":
        if ApplicationDefinition._client is None:
            raise AttributeError("Client has not been set: call _set_client prior to this method")
        App = ApplicationDefinition._client.App

        lookup: Dict[str, Union[str, int]]
        if isinstance(site, int):
            lookup = {"site_id": site}
        elif isinstance(site, str):
            lookup = {"site_name": site}
        elif isinstance(site, Site):
            assert site.id is not None
            lookup = {"site_id": site.id}
        else:
            raise ValueError("site must be an int, str, or Site object.")

        api_apps = App.objects.filter(**lookup)  # type: ignore
        apps_by_name = {}
        for app in api_apps:
            assert app.id is not None
            apps_by_name[app.name] = ApplicationDefinition.from_serialized(
                id=app.id,
                serialized_class=app.serialized_class,
            )
        return apps_by_name

    @staticmethod
    def load_app(apps_dir: Union[Path, str], class_path: str) -> Type["ApplicationDefinition"]:
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
    def sync(cls, rename_from: Optional[str] = None) -> None:
        if cls._client is None:
            raise AttributeError("Client has not been set: call _set_client prior to this method")

        app_dict = cls.to_dict()
        existing_app: Optional[App] = None
        if rename_from:
            existing_app = cls._client.App.objects.get(site_id=app_dict["site_id"], name=rename_from)
            logger.info(f"Renaming App(id={existing_app.id}): {rename_from} -> {app_dict['name']}")
        else:
            try:
                existing_app = cls._client.App.objects.get(site_id=app_dict["site_id"], name=app_dict["name"])
                logger.info(f"Updating App(id={existing_app.id}, name={app_dict['name']})")
            except App.DoesNotExist:
                pass

        if existing_app:
            for k, v in app_dict.items():
                setattr(existing_app, k, v)
            existing_app.save()
            cls.__app_id__ = existing_app.id
        else:
            new_app = cls._client.App.objects.create(**app_dict)
            logger.info(f"Created new App(id={new_app.id}, name={app_dict['name']})")
            cls.__app_id__ = new_app.id

    @classmethod
    def submit(cls):
        pass

    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        return dict(
            site_id=cls.resolve_site_id(),
            name=cls.__name__,
            serialized_class=serialize(cls),
            source_code=cls.source_code,
            description=(cls.__doc__ or "").strip(),
            parameters=cls.parameters,
            transfers=cls.transfers,
        )

    @staticmethod
    def from_serialized(id: int, serialized_class: str) -> "Type[ApplicationDefinition]":
        cls: Type[ApplicationDefinition] = deserialize(serialized_class)
        if not is_appdef(cls):
            raise TypeError(f"Deserialized {cls.__name__} is not an ApplicationDefinition subclass")
        cls.__app_id__ = id
        return cls
