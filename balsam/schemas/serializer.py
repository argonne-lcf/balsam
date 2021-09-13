import base64
import logging
from functools import lru_cache
from types import TracebackType
from typing import Any

import dill  # type: ignore
import dill.source  # type: ignore
from six import reraise
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


# Implementation of RemoteExceptionWrapper from parsl.apps.errors
# https://github.com/Parsl/parsl/blob/master/parsl/app/errors.py
class RemoteExceptionWrapper:
    def __init__(self, e_type: type, e_value: Exception, traceback: TracebackType) -> None:
        self.e_type = dill.dumps(e_type)
        self.e_value = dill.dumps(e_value)
        self.e_traceback = Traceback(traceback)

    def reraise(self) -> None:
        t = dill.loads(self.e_type)

        # the type is logged here before deserialising v and tb
        # because occasionally there are problems deserialising the
        # value (see #785, #548) and the fix is related to the
        # specific exception type.
        logger.debug("Reraising exception of type {}".format(t))
        v = dill.loads(self.e_value)
        tb = self.e_traceback.as_traceback()
        reraise(t, v, tb)
