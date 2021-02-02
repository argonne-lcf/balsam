from typing import Generic, Optional, TypeVar

from sqlalchemy.orm import Query

T = TypeVar("T")


class Paginator(Generic[T]):
    def __init__(self, limit: Optional[int] = None, offset: int = 0):
        self.limit = limit
        self.offset = offset

    def paginate(self, iterable: "Query[T]") -> "Query[T]":
        if self.limit is not None:
            return iterable[self.offset : self.offset + self.limit]  # type: ignore
        return iterable[self.offset :]  # type:ignore


class FilterSet(Generic[T]):
    def apply_filters(self, qs: "Query[T]") -> "Query[T]":
        return qs
