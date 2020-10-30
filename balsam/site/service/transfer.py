# flake8: noqa
from .service_base import BalsamService
from balsam.site import ProcessingJobSource, StatusUpdater
from balsam.site import ApplicationDefinition
from balsam.api.models import App
import multiprocessing
import signal
import queue
import logging

logger = logging.getLogger(__name__)


def stage_in(job):
    logger.debug(f"{job.cute_id} in stage_in")

    work_dir = job.working_directory
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)
        logger.debug(f"{job.cute_id} working directory {work_dir}")

    # stage in all remote urls
    # TODO: stage_in remote transfer should allow a list of files and folders,
    # rather than copying just one entire folder
    url_in = job.stage_in_url
    if url_in:
        logger.info(f"{job.cute_id} transfer in from {url_in}")
        try:
            transfer.stage_in(f"{url_in}", f"{work_dir}")
        except Exception as e:
            message = "Exception received during stage_in: " + str(e)
            raise BalsamTransitionError(message) from e

    # create unique symlinks to "input_files" patterns from parents
    # TODO: handle data flow from remote sites transparently
    matches = []
    parents = job.get_parents()
    input_patterns = job.input_files.split()
    logger.debug(f"{job.cute_id} searching parent workdirs for {input_patterns}")
    for parent in parents:
        parent_dir = parent.working_directory
        for pattern in input_patterns:
            path = os.path.join(parent_dir, pattern)
            matches.extend((parent.pk, match) for match in glob.glob(path))

    for parent_pk, inp_file in matches:
        basename = os.path.basename(inp_file)
        new_path = os.path.join(work_dir, basename)

        if os.path.exists(new_path):
            new_path += f"_{str(parent_pk)[:8]}"
        # pointing to src, named dst
        logger.info(f"{job.cute_id}   {new_path}  -->  {inp_file}")
        try:
            os.symlink(src=inp_file, dst=new_path)
        except FileExistsError:
            logger.warning(f"Symlink at {new_path} already exists; skipping creation")
        except Exception as e:
            raise BalsamTransitionError(
                f"Exception received during symlink: {e}"
            ) from e

    job.state = "STAGED_IN"
    logger.debug(f"{job.cute_id} stage_in done")


def stage_out(job):
    """copy from the local working_directory to the output_url """
    logger.debug(f"{job.cute_id} in stage_out")

    url_out = job.stage_out_url
    if not url_out:
        job.state = "JOB_FINISHED"
        logger.debug(f"{job.cute_id} no stage_out_url: done")
        return

    stage_out_patterns = job.stage_out_files.split()
    logger.debug(f"{job.cute_id} stage out files match: {stage_out_patterns}")
    work_dir = job.working_directory
    matches = []
    for pattern in stage_out_patterns:
        path = os.path.join(work_dir, pattern)
        matches.extend(glob.glob(path))

    if matches:
        logger.info(f"{job.cute_id} stage out files: {matches}")
        with tempfile.TemporaryDirectory() as stagingdir:
            try:
                for f in matches:
                    base = os.path.basename(f)
                    dst = os.path.join(stagingdir, base)
                    shutil.copyfile(src=f, dst=dst)
                    logger.info(f"staging {f} out for transfer")
                logger.info(f"transferring to {url_out}")
                transfer.stage_out(f"{stagingdir}/*", f"{url_out}/")
            except Exception as e:
                message = f"Exception received during stage_out: {e}"
                raise BalsamTransitionError(message) from e
    job.state = "JOB_FINISHED"
    logger.debug(f"{job.cute_id} stage_out done")
