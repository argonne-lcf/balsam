import inspect
import logging
import os
import re
import shlex
import sys
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

import jinja2
import jinja2.meta

from balsam.schemas import DeserializeError, JobState, SerializeError, deserialize, get_source, serialize

if TYPE_CHECKING:
    from balsam.client import RESTClient

    from .models import App, Job, Site

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


def chunk_str(s: str, chunksize: int) -> List[str]:
    num_chunks = len(s) // chunksize
    chunks = [s[i * chunksize : (i + 1) * chunksize] for i in range(num_chunks + 1)]
    chunks = [shlex.quote(chunk) for chunk in chunks]
    return chunks


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
    environment_variables: Dict[str, str]
    command_template: str
    parameters: Dict[str, Any]
    transfers: Dict[str, Any]
    cleanup_files: List[str]
    _default_params: Dict[str, str]
    _app_type: AppType
    run: Optional[Callable[..., Any]]
    _client: Optional["RESTClient"]

    @property
    def source_code(cls) -> str:
        return get_source(cls)

    @property
    def _Site(cls) -> Type["Site"]:
        if cls._client is None:
            raise AttributeError(f"Client has not been set: call {cls.__name__}._set_client prior to this method")
        return cls._client.Site

    @property
    def _App(cls) -> Type["App"]:
        if cls._client is None:
            raise AttributeError(f"Client has not been set: call {cls.__name__}._set_client prior to this method")
        return cls._client.App

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

        cls.parameters = {}
        for param_name, param in signature.parameters.items():
            if param_name == "self":
                continue
            cls.parameters[param_name] = metadata_from_signature(param)
        return cls

    def __new__(mcls, name: str, bases: Tuple[Any, ...], attrs: Dict[str, Any]) -> "ApplicationDefinitionMeta":
        attrs["parameters"] = attrs.pop("parameters", {}).copy()
        cls = super().__new__(mcls, name, bases, attrs)
        if not bases:
            return cls

        if "site" not in attrs:
            raise AttributeError(
                f"ApplicationDefinition {name} must contain the `site` attribute, set to a site id, name, or Site object"
            )

        has_command_template = isinstance(getattr(cls, "command_template", None), str)
        has_run_function = callable(getattr(cls, "run", None))

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


AppDefType = Type["ApplicationDefinition"]


class ApplicationDefinition(metaclass=ApplicationDefinitionMeta):
    class ModuleLoadError(Exception):
        pass

    ARG_CHUNK_SIZE: int = 128_000
    environment_variables: Dict[str, str] = {}
    command_template: str
    parameters: Dict[str, Any] = {}
    transfers: Dict[str, Any] = {}
    cleanup_files: List[str] = []
    _default_params: Dict[str, str]
    run: Optional[Callable[..., Any]] = None
    site: Union[int, str, "Site"]
    python_exe: str = sys.executable
    _site_id: Optional[int] = None
    _client: Optional["RESTClient"] = None
    _app_type: AppType
    __app_id__: Optional[int] = None
    _app_name_cache: Dict[Tuple[Optional[str], str], AppDefType] = {}
    _app_id_cache: Dict[int, AppDefType] = {}
    _serialized_class: Optional[str] = None

    @staticmethod
    def _set_client(client: "RESTClient") -> None:
        ApplicationDefinition._client = client

    def __init__(self, job: "Job") -> None:
        self.job = job

    def get_arg_str(self) -> str:
        if self._app_type == AppType.SHELL_CMD:
            return self._render_shell_command()
        elif self._app_type == AppType.PY_FUNC:
            return self._render_pyrunner_command()
        else:
            raise TypeError(f"Invalid _app_type: {self._app_type}")

    def get_environ_vars(self) -> Dict[str, str]:
        envs = os.environ.copy()
        envs.update(self.environment_variables)
        envs["BALSAM_JOB_ID"] = str(self.job.id)
        if self.job.threads_per_rank > 1:
            envs["OMP_NUM_THREADS"] = str(self.job.threads_per_rank)
        return envs

    def _render_pyrunner_command(self) -> str:
        assert self.__app_id__ is not None
        assert self.job._read_model is not None
        assert self._serialized_class is not None
        app_id = int(self.__app_id__)

        app_payload = self._serialized_class
        app_chunks = chunk_str(app_payload, self.ARG_CHUNK_SIZE)
        num_app_chunks = len(app_chunks)

        job_payload = self.job._read_model.json()
        job_chunks = chunk_str(job_payload, self.ARG_CHUNK_SIZE)

        args = f"{app_id} {num_app_chunks} {' '.join(app_chunks)} {' '.join(job_chunks)}"
        return f"{self.python_exe} -m balsam.site.launcher.python_runner {args}"

    def _render_shell_command(self) -> str:
        """
        Args:
            - arg_dict: value for each required parameter
        Returns:
            - str: shell command with safely-escaped parameters
            Use shlex.split() to split the result into args list.
            Do *NOT* use string.join on the split list: unsafe!
        """
        arg_dict = {**self._default_params, **self.job.get_parameters()}
        diff = set(self.parameters.keys()).difference(arg_dict.keys())
        if diff:
            raise ValueError(f"Missing required args: {diff} (only got: {arg_dict})")

        sanitized_args = {key: shlex.quote(str(arg_dict[key])) for key in self.parameters}
        return jinja2.Template(self.command_template).render(sanitized_args)

    def preprocess(self) -> None:
        self.job.state
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
        from balsam._api.models import Site

        if cls._site_id is not None:
            return cls._site_id
        elif isinstance(cls.site, str):
            site = cls._Site.objects.get(name=cls.site)
            assert site.id is not None
            cls._site_id = site.id
        elif isinstance(cls.site, Site):
            if cls.site.id is None:
                raise ValueError(f"{cls.__name__}.site does not have an ID set: {cls.site}")
            cls._site_id = cls.site.id
        else:
            if not isinstance(cls.site, int):
                raise ValueError(
                    f"{cls.__name__}.site must be a string, integer, or Site object. Got: {type(cls.site)}"
                )
            cls._site_id = cls.site
        return cls._site_id

    @staticmethod
    def load_by_site(site: Union[int, str, "Site"]) -> Dict[str, AppDefType]:
        AppModel: "Type[App]" = ApplicationDefinition._App

        lookup: Dict[str, Union[str, int]]
        if isinstance(site, int):
            lookup = {"site_id": site}
        elif isinstance(site, str):
            lookup = {"site_name": site}
        elif isinstance(site, ApplicationDefinition._Site):
            assert site.id is not None
            lookup = {"site_id": site.id}
        else:
            raise ValueError("site must be an int, str, or Site object.")

        api_apps = AppModel.objects.filter(**lookup)  # type: ignore
        apps_by_name = {}
        for app in api_apps:
            apps_by_name[app.name] = ApplicationDefinition.from_serialized(app)
            assert app.id is not None
            ApplicationDefinition._app_id_cache[app.id] = apps_by_name[app.name]
        return apps_by_name

    @classmethod
    def load_by_name(cls, app_name: str, site_name: Optional[str] = None) -> AppDefType:
        app_key = (site_name, app_name)
        if app_key not in cls._app_name_cache:
            logger.debug(f"App Cache miss: fetching app {app_key}")
            app: "App" = cls._App.objects.get(site_name=site_name, name=app_name)
            assert app.id is not None
            app_def = cls.from_serialized(app)
            cls._app_name_cache[app_key] = app_def
            cls._app_id_cache[app.id] = app_def
        return cls._app_name_cache[app_key]

    @classmethod
    def load_by_id(cls, app_id: int) -> AppDefType:
        if app_id not in cls._app_id_cache:
            logger.debug(f"App Cache miss: fetching app {app_id}")
            api_app: "App" = cls._App.objects.get(id=app_id)
            app_def = cls.from_serialized(api_app)
            cls._app_id_cache[app_id] = app_def
        return cls._app_id_cache[app_id]

    @classmethod
    def sync(cls, rename_from: Optional[str] = None) -> None:
        app_dict = cls.to_dict()
        existing_app: Optional[App] = None
        AppModel: Type["App"] = cls._App
        if rename_from:
            existing_app = AppModel.objects.get(site_id=app_dict["site_id"], name=rename_from)
            logger.info(f"Renaming App(id={existing_app.id}): {rename_from} -> {app_dict['name']}")
        else:
            try:
                existing_app = AppModel.objects.get(site_id=app_dict["site_id"], name=app_dict["name"])
                logger.info(f"Updating App(id={existing_app.id}, name={app_dict['name']})")
            except AppModel.DoesNotExist:
                pass

        if existing_app:
            for k, v in app_dict.items():
                setattr(existing_app, k, v)
            existing_app.save()
            cls.__app_id__ = existing_app.id
            assert existing_app.id is not None
            cls._app_id_cache[existing_app.id] = cls
        else:
            new_app = AppModel.objects.create(**app_dict)
            logger.info(f"Created new App(id={new_app.id}, name={app_dict['name']})")
            cls.__app_id__ = new_app.id
            assert new_app.id is not None
            cls._app_id_cache[new_app.id] = cls

    @classmethod
    def submit(
        cls,
        workdir: Path,
        tags: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        return_code: Optional[int] = None,
        num_nodes: int = 1,
        ranks_per_node: int = 1,
        threads_per_rank: int = 1,
        threads_per_core: int = 1,
        launch_params: Optional[Dict[str, str]] = None,
        gpus_per_rank: float = 0,
        node_packing_count: int = 1,
        wall_time_min: int = 0,
        parent_ids: Set[int] = set(),
        transfers: Optional[Dict[str, str]] = None,
        save: bool = True,
        **app_params: Any,
    ) -> "Job":
        """
        Construct a new Job object.  If save=True (default), the Job
        will be saved to the API automatically.  Use save=False to create
        in-memory Jobs only, which can then be created in bulk by passing
        into Job.objects.bulk_create().

        workdir:            Job path relative to site data/ folder.
        tags:               Custom key:value string tags.
        data:               Arbitrary JSON-able data dictionary.
        return_code:        Return code from last execution of this Job.
        num_nodes:          Number of compute nodes needed.
        ranks_per_node:     Number of MPI processes per node.
        threads_per_rank:   Logical threads per process.
        threads_per_core:   Logical threads per CPU core.
        launch_params:      Optional pass-through parameters to MPI application launcher.
        gpus_per_rank:      Number of GPUs per process.
        node_packing_count: Maximum number of concurrent runs per node.
        wall_time_min:      Optional estimate of Job runtime. All else being equal, longer Jobs tend to run first.
        parent_ids:         Set of parent Job IDs (dependencies).
        transfers:          TransferItem dictionary. One key:JobTransferItem pair for each slot defined on the App.
        save:               Whether to save the Job to the API immediately.
        **app_params:       Parameters passed to App at runtime.
        """
        if cls.__app_id__ is None:
            cls.sync()
        assert cls.__app_id__ is not None
        assert cls._client is not None
        job_kwargs = {k: v for k, v in locals().items() if k not in ["cls", "__class__"] and v is not None}
        job_kwargs["parameters"] = app_params
        job_kwargs["app_id"] = cls.__app_id__
        job = cls._client.Job(**job_kwargs)
        if save:
            job.save()
        return job

    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        try:
            serialized_class = serialize(cls)
        except SerializeError as exc:
            logger.error(f"Please fix {cls.__name__}: can't serialize class due to: {exc}")
            raise
        cls._serialized_class = serialized_class
        return dict(
            site_id=cls.resolve_site_id(),
            name=cls.__name__,
            serialized_class=serialized_class,
            source_code=cls.source_code,
            description=(cls.__doc__ or "").strip(),
            parameters=cls.parameters,
            transfers=cls.transfers,
        )

    @staticmethod
    def from_serialized(app: "App") -> AppDefType:
        if app.id is None:
            raise ValueError("Cannot deserialize App with id=None")

        try:
            cls: AppDefType = deserialize(app.serialized_class)
        except DeserializeError as exc:
            raise DeserializeError(f"Failed to deserialize App(id={app.id}, name={app.name}): {exc}") from exc
        if not is_appdef(cls):
            raise TypeError(f"Deserialized {cls.__name__} is not an ApplicationDefinition subclass")

        cls.__app_id__ = app.id
        cls._serialized_class = app.serialized_class
        return cls
