from balsam import site
from .util import file_watcher
import threading

FILE_WATCHER_DELAY = 2

def sandbox_exec_file(path):
    local_vars, global_vars = {}, {}
    try:
        with open(path) as fp: module_contents = fp.read()
        exec(module_contents, globals=global_vars, locals=local_vars)
    except Exception as e:
        logger.exception(
            f'An exception occured inside {path}. '
            'Please fix and re-save the file.'
        )
        return {}
    else:
        return global_vars

def app_modified(filepath):
    global_vars = sandbox_exec_file(filepath)
    for k,v in global_vars.items():

def main():
    site.setup()

    app_watcher = threading.Thread(
        target=file_watcher.watcher,
        args=(
            app_modified,
            site.APPS_PATH,
            "*.py",
            FILE_WATCHER_DELAY
        ),
        daemon=True
    )
    app_watcher.start()



if __name__ == "__main__":
    main()
