import json
import sys
from typing import Any, Dict, List, Tuple, Type

from balsam._api.app import ApplicationDefinition, is_appdef
from balsam._api.models import App, Job
from balsam.schemas import RemoteExceptionWrapper, serialize


def unpack_chunks(
    app_id: int, num_app_chunks: int, chunks: List[str]
) -> Tuple[Type[ApplicationDefinition], Job, Dict[str, Any]]:
    # sys.argv contains the serialized ApplicationDefinition and app_id (not the JSON app representation)
    app_encoded = "".join(chunks[:num_app_chunks])
    app = App(_api_data=True, id=app_id, site_id=0, name="AppName", serialized_class=app_encoded, source_code="")
    app_def = ApplicationDefinition.from_serialized(app)
    if not is_appdef(app_def):
        raise ValueError(f"{app_def} is not an ApplicationDefinition; type is {type(app_def)}")

    # sys.argv contains the full JSON representation of the Job:
    job_encoded = "".join(chunks[num_app_chunks:])
    job_dict: Dict[str, Any] = json.loads(job_encoded)
    job = Job._from_api(job_dict)
    params = job.get_parameters()

    return app_def, job, params


def log_exception(exc: RemoteExceptionWrapper) -> None:
    print("BALSAM-EXCEPTION", serialize(exc), flush=True)


def log_result(ret_val: Any) -> None:
    print("BALSAM-RETURN-VALUE", serialize(ret_val), flush=True)


def main(app_id: int, num_app_chunks: int, chunks: List[str]) -> None:
    try:
        app_cls, job, params = unpack_chunks(app_id, num_app_chunks, chunks)
        app = app_cls(job)
        if not callable(app.run):
            raise AttributeError(f"ApplicationDefinition {app_cls} does not have a run() function")
        return_value = app.run(**params)
        log_result(return_value)
    except Exception:
        exc = RemoteExceptionWrapper(*sys.exc_info())
        log_exception(exc)
        raise


if __name__ == "__main__":
    # python -m balsam.site.launcher.python_runner APP_ID NUM_APP_CHUNKS [APP_B64_CHUNKS] [JOB_JSON_CHUNKS]
    app_id = int(sys.argv[1])
    num_app_chunks = int(sys.argv[2])
    main(app_id, num_app_chunks, sys.argv[3:])
