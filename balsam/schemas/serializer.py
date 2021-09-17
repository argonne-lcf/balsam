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


class SerializeError(ValueError):
    pass


class DeserializeError(ValueError):
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
        try:
            return _cached_serialize(obj)
        except TypeError:
            return _serialize(obj)
    except Exception as exc:
        logger.exception(f"Failed to serialize {obj}")
        raise SerializeError(str(exc)) from exc


@lru_cache(256)
def deserialize(payload: str) -> Any:
    if not payload:
        raise EmptyPayload
    try:
        decoded = base64.b64decode(payload)
        return dill.loads(decoded)
    except Exception as exc:
        logger.exception("Failed to deserialize")
        raise DeserializeError(str(exc)) from exc


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

    try:
        exc, tb = deserialize(payload)
    except DeserializeError as deser_exc:
        logger.error(f"An exception was transmitted, but it could not be unpacked here due to: {deser_exc}")
        logger.error("You may find the original error in the job.out file of the Job's working directory")
        raise

    if tb is not None:
        raise exc.with_traceback(tb.as_traceback())
    else:
        raise exc
