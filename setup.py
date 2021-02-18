#!/usr/bin/env python
# -*- coding: utf-8 -*-

import shutil
import subprocess
from pathlib import Path

from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install
from setuptools.config import read_configuration

# Do not remove this import: monkey-patches the easy_install ScriptWriter
import fastentrypoints  # noqa: F401

setup_cfg = Path(__file__).parent.joinpath("setup.cfg")
conf_dict = read_configuration(setup_cfg)


class PostDevelopCommand(develop):
    def run(self):
        develop.run(self)
        setup_autocomplete()


class PostInstallCommand(install):
    def run(self):
        install.run(self)
        setup_autocomplete()


def setup_autocomplete():
    balsam_bin = Path(shutil.which("balsam")).parent
    completion_path = balsam_bin / "completion.sh"
    subprocess.run(
        f"_BALSAM_COMPLETE=source balsam > {completion_path}",
        shell=True,
        executable="/bin/bash",
    )


setup(
    cmdclass={"develop": PostDevelopCommand, "install": PostInstallCommand},
)
