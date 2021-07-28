from typing import Generic, Optional, TypeVar

from fastapi import Query as APIQuery
from sqlalchemy.orm import Query as SQLQuery

T = TypeVar("T")


class Paginator(Generic[T]):
    def __init__(
        self,
        limit: Optional[int] = APIQuery(None, description="Maximum number of items to return."),
        offset: int = APIQuery(0, description="Starting index from which to retrieve results."),
    ):
        self.limit = limit
        self.offset = offset

    def paginate(self, iterable: "SQLQuery[T]") -> "SQLQuery[T]":
        if self.limit is not None:
            return iterable[self.offset : self.offset + self.limit]  # type: ignore
        return iterable[self.offset :]  # type:ignore

    def __new__(
        cls,
        limit: Optional[int] = APIQuery(None, description="Maximum number of items to return."),
        offset: int = APIQuery(0, description="Starting index from which to list items."),
    ) -> "Paginator[T]":
        return super().__new__(cls)  # type: ignore
