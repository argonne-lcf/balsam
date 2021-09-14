import base64
import logging
from functools import lru_cache
from typing import Any, Optional

import dill  # type: ignore
import dill.source  # type: ignore
from tblib import Traceback  # type: ignore

logger = logging.getLogger(__name__)


class EmptyPayload(ValueError):
    pass


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
    if not payload:
        raise EmptyPayload
    decoded = base64.b64decode(payload)
    return dill.loads(decoded)


# See RemoteExceptionWrapper in parsl.apps.errors
# https://github.com/Parsl/parsl/blob/master/parsl/app/errors.py
def serialize_exception(exc: Exception) -> str:
    if exc.__traceback__ is not None:
        tb = Traceback(exc.__traceback__)
    else:
        tb = None
    return serialize((exc, tb))


def raise_from_serialized(payload: str) -> None:
    exc: Exception
    tb: Optional[Traceback]
    exc, tb = deserialize(payload)
    if tb is not None:
        raise exc.with_traceback(tb.as_traceback())
    else:
        raise exc
