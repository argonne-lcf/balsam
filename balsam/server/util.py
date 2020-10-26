from typing import Optional


class Paginator:
    def __init__(self, limit: Optional[int] = None, offset: int = 0):
        self.limit = limit
        self.offset = offset

    def paginate(self, iterable):
        if self.limit is not None:
            return iterable[self.offset : self.offset + self.limit]
        return iterable[self.offset :]
