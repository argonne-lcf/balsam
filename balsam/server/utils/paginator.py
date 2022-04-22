from typing import Generic, TypeVar, cast

from fastapi import Query as APIQuery
from sqlalchemy.orm import Query as SQLQuery
from sqlalchemy.sql import Select

from balsam.schemas import MAX_PAGE_SIZE

T = TypeVar("T")


class Paginator(Generic[T]):
    """Paging data class"""

    def __init__(
        self,
        limit: int = APIQuery(MAX_PAGE_SIZE, le=MAX_PAGE_SIZE, description="Maximum number of items to return."),
        offset: int = APIQuery(0, ge=0, description="Starting index from which to retrieve results."),
    ):
        self.limit = limit
        self.offset = offset

    def paginate(self, iterable: "SQLQuery[T]") -> "SQLQuery[T]":
        return cast("SQLQuery[T]", iterable[self.offset : self.offset + self.limit])

    def paginate_core(self, stmt: "Select") -> "Select":
        if self.limit is not None:
            stmt = stmt.limit(self.limit)
        return stmt.offset(self.offset)

    def __new__(
        cls,
        limit: int = APIQuery(MAX_PAGE_SIZE, le=MAX_PAGE_SIZE, description="Maximum number of items to return."),
        offset: int = APIQuery(0, ge=0, description="Starting index from which to list items."),
    ) -> "Paginator[T]":
        return super().__new__(cls)
