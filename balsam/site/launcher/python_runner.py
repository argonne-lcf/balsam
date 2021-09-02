from typing import List, Type, Dict, Any, Tuple
import dill  # type: ignore
import json
import base64
import sys

from balsam.site.app import ApplicationDefinition, is_appdef
from balsam._api.models import Job
from balsam.schemas import serialize


def unpack_chunks(num_app_chunks: int, chunks: List[str]) -> Tuple[Type[ApplicationDefinition], Job]:
    app_encoded = "".join(chunks[:num_app_chunks])
    job_encoded = "".join(chunks[num_app_chunks:])
    app_cls: Type[ApplicationDefinition] = dill.loads(base64.b64decode(app_encoded))
    job_dict: Dict[str, Any] = json.loads(job_encoded)
    job = Job._from_api(job_dict)
    assert is_appdef(app_cls)
    return app_cls, job


def log_exception(exc: Exception) -> None:
    print("BALSAM-EXCEPTION", serialize())


def log_result(ret_val: Any) -> None:
    pass


def main(num_app_chunks: int, chunks: List[str]) -> None:
    app_cls, job = unpack_chunks(num_app_chunks, chunks)
    app = app_cls(job)
    try:
        params = job.unpack_parameters()
        return_value = app.run(**params)
    except Exception as exc:
        log_exception(sys.exc_info())
    else:
        log_result(return_value)


if __name__ == "__main__":
    num_app_chunks = int(sys.argv[1])
    main(num_app_chunks, sys.argv[2:])
