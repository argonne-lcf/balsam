import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Set, Tuple, Union

from balsam._api.model import Field
from balsam._api.models import Job as _APIJob, JobManager, JobQuery
from balsam.schemas import JobTransferItem, deserialize, serialize

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from balsam._api.models import App


class Job:

    _app_cache: Dict[Tuple[str, str], "App"] = {}
    _app_id_cache: Dict[int, "App"] = {}

    objects: "JobManager"
    workdir: Field[Path]
    app_id: Field[int]
    parent_ids: Field[Set[int]]

    def __init__(
        self,
        workdir: Path,
        app: Union[str, int, App],
        site_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, str]] = None,
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
        transfers: Optional[Dict[str, Union[str, JobTransferItem]]] = None,
        **kwargs: Any,
    ) -> None:
        serialized_parameters = serialize(parameters)
        app_id = self._resolve_app_id(app, site_name)
        transfer_items = self._validate_transfers(transfers or {})

        self._job = _APIJob(
            workdir=workdir,
            app_id=app_id,
            tags=tags,
            serialized_parameters=serialized_parameters,
            data=data,
            return_code=return_code,
            num_nodes=num_nodes,
            ranks_per_node=ranks_per_node,
            threads_per_rank=threads_per_rank,
            threads_per_core=threads_per_core,
            launch_params=launch_params,
            gpus_per_rank=gpus_per_rank,
            node_packing_count=node_packing_count,
            wall_time_min=wall_time_min,
            parent_ids=parent_ids,
            transfers=transfer_items,
        )

    def _fetch_app_by_name(self, app_name: str, site_name: str) -> "App":
        app_key = (site_name, app_name)
        if app_key not in Job._app_cache:
            AppManager = self.objects._client.App.objects
            logger.debug(f"App Cache miss: fetching app {app_key}")
            app = AppManager.get(site_name=site_name, class_path=app_name)
            assert app.id is not None
            Job._app_cache[app_key] = app
            Job._app_id_cache[app.id] = app
        return Job._app_cache[app_key]

    def _fetch_app_by_id(self) -> "App":
        if self.app_id is None:
            raise ValueError("Cannot fetch by app ID; is None")
        if self.app_id not in Job._app_id_cache:
            AppManager = self.objects._client.App.objects
            logger.debug(f"App Cache miss: fetching app {self.app_id}")
            app = AppManager.get(id=self.app_id)
            Job._app_id_cache[self.app_id] = app
        return Job._app_id_cache[self.app_id]

    def _resolve_app_id(self, app: Union[str, int, App], site_name: Optional[str]) -> int:
        if isinstance(app, int):
            self.app_id = app
        elif isinstance(app, str):
            app = self._fetch_app_by_name(app, site_name)
            self.app_id = app.id
        else:
            if app.id is None:
                raise ValueError(f"app.id is None: {app}")
            self.app_id = app.id
            Job._app_id_cache[self.app_id] = app
        return self.app_id

    @staticmethod
    def _validate_transfers(transfers: Dict[str, Union[str, JobTransferItem]]) -> Dict[str, JobTransferItem]:
        validated = {}
        for key, val in transfers.items():
            if isinstance(val, str):
                location_alias, path = val.split(":", 1)
                validated[key] = JobTransferItem(location_alias=location_alias, path=path)
            else:
                validated[key] = val
        return validated

    @property
    def app(self) -> "App":
        return self._fetch_app_by_id()

    @property
    def site_id(self) -> int:
        return self._fetch_app_by_id().site_id

    def resolve_workdir(self, data_path: Path) -> Path:
        return data_path.joinpath(self.workdir)

    def parent_query(self) -> "JobQuery":
        return self.objects.filter(id=list(self.parent_ids))
