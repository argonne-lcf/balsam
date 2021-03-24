from balsam.site import ApplicationDefinition


class Eig(ApplicationDefinition):
    """
    Runs XPCS-Eigen on an (H5, IMM) file pair from a remote location.

    Jobs do not require any command template parameters; instead,
    users provide `h5_in`, `imm_in`, and `h5_out` transfer items.
    """

    command_template = "python /projects/WorkExpFacil/msalim/eig/random-eig.py {{ inp_file }}"
    environment_variables = {}
    parameters = {}
    cleanup_files = ["*.npy"]
    transfers = {
        "matrix": {
            "required": True,
            "direction": "in",
            "local_path": ".",
            "description": "Input npy matrix",
            "recursive": False,
        },
        "eigvals": {
            "required": True,
            "direction": "out",
            "local_path": "result.npy",
            "description": "Output npy eigvals",
            "recursive": False,
        },
    }

    def shell_preamble(self):
        return ["module load miniconda-3"]
