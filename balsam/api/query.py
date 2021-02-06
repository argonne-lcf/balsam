from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional, Tuple, TypeVar, Union, cast

from .model import BalsamModel

T = TypeVar("T", bound=BalsamModel)

if TYPE_CHECKING:
    from .manager import Manager

    U = TypeVar("U", bound="Query")  # type: ignore

REPR_OUTPUT_SIZE = 20


class Query(Iterable[T]):
    def __init__(self, manager: "Manager[T]") -> None:
        self._manager: "Manager[T]" = manager
        self._result_cache: Optional[List[T]] = None
        self._filters: Dict[str, Any] = {}
        self._order_fields: Tuple[str, ...] = tuple()
        self._count: Optional[int] = None
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None

    def __repr__(self) -> str:
        data = list(self[: REPR_OUTPUT_SIZE + 1])  # type: ignore
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."  # type: ignore
        return "<%s %r>" % (self.__class__.__name__, data)

    def __len__(self) -> int:
        self._fetch_cache()
        assert isinstance(self._result_cache, list)
        return len(self._result_cache)

    def __bool__(self) -> bool:
        self._fetch_cache()
        return bool(self._result_cache)

    def __setitem__(self, k: int, v: T) -> None:
        if not isinstance(k, int):
            raise TypeError("Item assignment only support for int index")

        if self._result_cache is None:
            self._fetch_cache()

        assert isinstance(self._result_cache, list)
        self._result_cache[k] = v

    def __getitem__(self, k: Union[int, slice]) -> Union[List[T], T, "Query[T]"]:
        """
        Retrieve an item or slice from the set of results.
        """
        if not isinstance(k, (int, slice)):
            raise TypeError("Query indices must be integers or slices, not %s." % type(k).__name__)
        assert (not isinstance(k, slice) and (k >= 0)) or (
            isinstance(k, slice) and (k.start is None or k.start >= 0) and (k.stop is None or k.stop >= 0)
        ), "Negative indexing is not supported."

        if self._result_cache is not None:
            return self._result_cache[k]

        if isinstance(k, slice):
            start: Optional[int]
            stop: Optional[int]
            clone = self._clone()
            if k.start is not None:
                start = int(k.start)
            else:
                start = None
            if k.stop is not None:
                stop = int(k.stop)
            else:
                stop = None
            clone._set_limits(start, stop)
            return list(clone)[start : stop : k.step] if k.step else clone
        else:
            clone = self._clone()
            clone._set_limits(k, k + 1)
            clone._fetch_cache()
            assert clone._result_cache is not None
            return clone._result_cache[0]

    @property
    def _is_sliced(self) -> bool:
        return not (self._limit is None and self._offset is None)

    def _clone(self: "U") -> "U":
        clone = type(self)(manager=self._manager)
        clone._filters = self._filters.copy()
        clone._order_fields = self._order_fields
        clone._limit = self._limit
        clone._offset = self._offset
        return clone

    def _set_limits(self, start: Optional[int], stop: Optional[int]) -> None:
        if start is None:
            start = 0
        self._offset = start
        if stop is not None:
            self._limit = stop - start

    def __iter__(self) -> Iterator[T]:
        self._fetch_cache()
        assert isinstance(self._result_cache, list)
        return iter(self._result_cache)

    def _fetch_cache(self) -> None:
        if self._result_cache is not None:
            return
        instances, count = self._manager._get_list(
            filters=self._filters,
            ordering=self._order_fields,
            limit=self._limit,
            offset=self._offset,
        )
        if count is not None:
            self._count = count
        self._result_cache = instances

    def _filter(self: "U", **kwargs: Any) -> "U":
        # TODO: kwargs should expand to the set of filterable model fields
        if self._is_sliced:
            raise AttributeError("Cannot filter a sliced Query")
        clone = self._clone()

        for key, val in kwargs.items():
            if isinstance(val, dict):
                kwargs[key] = [f"{k}:{v}" for k, v in val.items()]
        clone._filters.update(kwargs)
        return clone

    def _order_by(self: "U", *fields: str) -> "U":
        # TODO: should validate that only order-able fields are accepted
        if self._is_sliced:
            raise AttributeError("Cannot re-order a sliced Query")
        clone = self._clone()
        clone._order_fields = tuple(fields)
        return clone

    # Methods that do not return a Query
    # **********************************
    def _get(self, **kwargs: Any) -> T:
        # TODO: kwargs should expand to the set of filterable model fields
        clone: Query[T] = self._filter(**kwargs)
        clone._fetch_cache()
        assert clone._result_cache is not None
        results: List[T] = list(clone)
        nobj = len(results)
        if nobj == 1:
            return results[0]
        elif nobj == 0:
            raise self._manager._model_class.DoesNotExist
        else:
            raise self._manager._model_class.MultipleObjectsReturned(nobj)

    def first(self) -> T:
        return cast(T, self[0])

    def count(self) -> Optional[int]:
        if self._count is None:
            _, _count = self._manager._get_list(filters=self._filters, limit=0, offset=0, ordering=None)
            self._count = _count
        return self._count

    def _update(self, **kwargs: Any) -> List[T]:
        # TODO: kwargs should expand to a set of allowed update_fields
        return self._manager._do_bulk_update_query(patch=kwargs, filters=self._filters)

    def delete(self) -> None:
        return self._manager._do_bulk_delete(filters=self._filters)
