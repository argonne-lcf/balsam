import logging
from math import ceil
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union

from balsam.schemas import MAX_ITEMS_PER_BULK_OP, MAX_PAGE_SIZE

from .model import BalsamModel
from .query import Query

if TYPE_CHECKING:
    from balsam.client import RESTClient

FILTER_CHUNK_SIZE = 500

logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BalsamModel)
U = TypeVar("U")


def chunk_list(items: List[U], chunk_size: int) -> List[List[U]]:
    num_chunks = ceil(len(items) / chunk_size)
    return [items[n * chunk_size : (n + 1) * chunk_size] for n in range(num_chunks)]


class Manager(Generic[T]):
    _model_class: Type[T]
    _query_class: Type[Query[T]]
    _bulk_create_enabled: bool
    _bulk_update_enabled: bool
    _bulk_delete_enabled: bool
    _api_path: str

    def __init__(self, client: "RESTClient") -> None:
        self._client = client
        self._model_class.objects = self

    def all(self) -> Query[T]:
        return self._query_class(manager=self)

    def count(self) -> Optional[int]:
        return self.all().count()

    def first(self) -> T:
        return self.all().first()

    def _create(self, instance: Optional[T] = None, **data: Any) -> T:
        # Pass raw dict through the BalsamModel constructor for validation
        if instance is None:
            instance = self._model_class(**data)
        if self._bulk_create_enabled:
            created_list = self.bulk_create([instance])
            created = created_list[0]
        else:
            assert instance._create_model is not None
            data = instance._create_model.dict()
            created = self._client.post(self._api_path, **data)
            created = self._model_class._from_api(created)
        return created

    def bulk_create(self, instances: List[T]) -> List[T]:
        """Returns a list of newly created instances"""
        if not self._bulk_create_enabled:
            raise NotImplementedError("The {self._model_class.__name__} API does not offer bulk_create")

        if not isinstance(instances, list):
            raise TypeError(f"instances must be a list of {self._model_class.__name__} instances")

        assert self._model_class._create_model_cls is not None
        assert all(
            isinstance(obj._create_model, self._model_class._create_model_cls) for obj in instances
        ), f"bulk_create requires all items to be instances of {self._model_class.__name__}"

        data_list = [obj._create_model.dict() for obj in instances]  # type: ignore # (checked in the assert above)
        response_data: List[Dict[str, Any]] = []

        for chunk in chunk_list(data_list, chunk_size=MAX_ITEMS_PER_BULK_OP):
            res = self._client.bulk_post(self._api_path, chunk)
            response_data.extend(res)

        # Cannot update in-place: no IDs to perform the mapping yet
        return [self._model_class._from_api(dat) for dat in response_data]

    def bulk_update(self, instances: List[T]) -> None:
        """
        Perform a bulk patch of instances from the modified `instances` list and set of
        `update_fields`. Modifies the instances list in-place and returns None.
        """
        if not self._bulk_update_enabled:
            raise NotImplementedError("The {self._model_class.__name__} API does not offer bulk_update")

        patch_list = []
        for obj in instances:
            if obj._update_model is not None:
                patch = {"id": obj.id, **obj._update_model.dict(exclude_unset=True)}
                patch_list.append(patch)

        if not patch_list:
            logger.debug("bulk-update: patch_list is empty (nothing to update!)")
            return

        logger.debug(f"Performing bulk-update to {self._api_path}\n{patch_list}")
        response_data = []

        for chunk in chunk_list(patch_list, chunk_size=MAX_ITEMS_PER_BULK_OP):
            res = self._client.bulk_patch(self._api_path, chunk)
            if isinstance(res, int):
                response_data.append(res)
            else:
                response_data.extend(res)

        if isinstance(response_data[0], int):
            logger.info(f"Updated {response_data} items: data was not updated in place.")
            return

        response_map = {item["id"]: item for item in response_data}
        logger.debug(f"bulk-update response map: {response_map}")
        # Use response_map to update instances in-place
        for obj in instances:
            if obj.id in response_map:
                logger.debug(f"refreshing object id={obj.id} from response_map")
                obj._refresh_from_dict(response_map[obj.id])

    def _build_query_params(
        self,
        filters: Dict[str, Any],
        ordering: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Dict[str, Any]:
        d = {}
        d.update(filters)
        if ordering is not None:
            d.update(ordering=ordering)
        if limit is not None:
            d.update(limit=limit)
        if offset is not None:
            d.update(offset=offset)
        return d

    @staticmethod
    def _chunk_filters(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        param_to_chunk = next(
            (
                (name, val)
                for (name, val) in filters.items()
                if isinstance(val, (list, tuple, set)) and len(val) > FILTER_CHUNK_SIZE
            ),
            None,
        )
        if param_to_chunk is None:
            return [filters]

        name, param_list = param_to_chunk
        return [{**filters, name: chunk} for chunk in chunk_list(list(param_list), FILTER_CHUNK_SIZE)]

    def _unpack_list_response(self, response_data: Dict[str, Any]) -> Tuple[int, List[Dict[str, Any]]]:
        count = response_data["count"]
        results = response_data["results"]
        return count, results

    def _fetch_pages(
        self, filters: Dict[str, Any], ordering: Optional[str], limit: Optional[int], offset: Optional[int]
    ) -> Tuple[int, List[Dict[str, Any]]]:
        base_offset = 0 if offset is None else offset
        page_size = MAX_PAGE_SIZE if limit is None else min(limit, MAX_PAGE_SIZE)

        # Fetch the first page of data and total item count
        query_params = self._build_query_params(filters, ordering, limit=page_size, offset=base_offset)
        response_data = self._client.get(self._api_path, **query_params)
        count, results = self._unpack_list_response(response_data)

        num_to_fetch = count if limit is None else min(limit, count)
        num_pages = ceil(num_to_fetch / MAX_PAGE_SIZE)

        # If there is more than 1 page of data to fetch
        for page_no in range(1, num_pages):
            to_fetch = min(page_size, num_to_fetch - len(results))
            query_params = self._build_query_params(
                filters, ordering, limit=to_fetch, offset=base_offset + (page_no * page_size)
            )
            response_data = self._client.get(self._api_path, **query_params)
            _, page = self._unpack_list_response(response_data)
            results.extend(page)
        return count, results

    def _get_list(
        self,
        filters: Dict[str, Any],
        ordering: Optional[str],
        limit: Optional[int],
        offset: Optional[int],
    ) -> Tuple[List[T], int]:
        filter_chunks = self._chunk_filters(filters)
        full_count: int = 0
        full_results: List[Dict[str, Any]] = []

        # Added complexity: we handle the case that one URL query
        # parameter is too long: chunk query into multiple GETs passing subsets
        # of the sequence (e.g. filter by list of 100k job ids will result in 196 requests
        # being stitched together)
        for filter_chunk in filter_chunks:
            count, results = self._fetch_pages(filter_chunk, ordering, limit, offset)
            full_count += count
            full_results.extend(results)
        if ordering and len(filter_chunks) > 1:
            order_key, reverse = (ordering.lstrip("-"), True) if ordering.startswith("-") else (ordering, False)
            full_results = sorted(full_results, key=lambda r: r[order_key], reverse=reverse)  # type: ignore
        instances = [self._model_class._from_api(dat) for dat in full_results]
        return instances, full_count

    def _do_update(self, instance: T) -> None:
        assert instance._update_model is not None
        update_data = instance._update_model.dict(exclude_unset=True)
        response_data = self._client.put(
            self._api_path + f"{instance.id}",
            **update_data,
        )
        if isinstance(response_data, dict):
            instance._refresh_from_dict(response_data)
        elif instance._read_model is not None:
            instance._read_model = instance._read_model.copy(update=update_data)
            instance._set_clean()

    def _do_bulk_update_query(self, patch: Dict[str, Any], filters: Dict[str, Any]) -> Union[int, List[T]]:
        if not self._bulk_update_enabled:
            raise NotImplementedError(f"The {self._model_class.__name__} API does not offer bulk updates")

        _, items = self._fetch_pages(filters, ordering=None, limit=None, offset=None)
        update_ids = [item["id"] for item in items]

        response_data = []
        for ids_chunk in chunk_list(update_ids, chunk_size=FILTER_CHUNK_SIZE):
            res = self._client.bulk_put(self._api_path, patch, id=ids_chunk)
            if isinstance(res, int):
                response_data.append(res)
            else:
                response_data.extend(res)

        if response_data and isinstance(response_data[0], int):
            return sum(response_data)
        instances = [self._model_class._from_api(dat) for dat in response_data]
        return instances

    def _do_delete(self, instance: T) -> None:
        self._client.delete(self._api_path + f"{instance.id}")
        if instance._read_model is not None:
            instance._read_model.id = None  # type: ignore

    def _do_bulk_delete(self, filters: Dict[str, Any]) -> Union[int, None]:
        if not self._bulk_delete_enabled:
            raise NotImplementedError(f"The {self._model_class.__name__} API does not offer bulk deletes")
        query_params = self._build_query_params(filters)
        response = self._client.bulk_delete(self._api_path, **query_params)
        if isinstance(response, int):
            return response
        return None
