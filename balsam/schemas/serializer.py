import base64
from typing import Any
import dill  # type: ignore
import dill.source  # type: ignore


def get_source(obj: Any) -> str:
    source: str = dill.source.getsource(obj, lstrip=True)
    return source


def serialize(obj: Any) -> str:
    dump: bytes = dill.dumps(obj, recurse=True)
    return base64.b64encode(dump).decode("utf-8")


def deserialize(payload: str) -> Any:
    decoded = base64.b64decode(payload)
    return dill.loads(decoded)
