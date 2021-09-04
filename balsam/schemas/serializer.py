import base64
from functools import lru_cache
from typing import Any

import dill  # type: ignore
import dill.source  # type: ignore


def get_source(obj: Any) -> str:
    source: str = dill.source.getsource(obj, lstrip=True)
    return source


def _serialize(obj: Any) -> str:
    dump: bytes = dill.dumps(obj, recurse=True)
    return base64.b64encode(dump).decode("utf-8")


_cached_serialize = lru_cache(256)(_serialize)


def serialize(obj: Any) -> str:
    try:
        return _cached_serialize(obj)
    except TypeError:
        return _serialize(obj)


@lru_cache(256)
def deserialize(payload: str) -> Any:
    decoded = base64.b64decode(payload)
    return dill.loads(decoded)
