import logging

logger = logging.getLogger(__name__)


def sandbox_exec_file(path):
    local_vars, global_vars = {}, {}
    try:
        with open(path) as fp:
            module_contents = fp.read()
        exec(module_contents, globals=global_vars, locals=local_vars)
    except Exception:
        logger.exception(f"An exception occured inside {path}. " "Please fix and re-save the file.")
        return {}
    else:
        return global_vars
