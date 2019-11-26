from setuptools import setup, Extension
from setuptools.config import read_configuration
from os import path, environ

here = path.dirname(__file__)
conf_dict = read_configuration(path.join(here, 'setup.cfg'))

install_requires = conf_dict['options']['install_requires']

if environ.get('READTHEDOCS') == 'True':
    install_requires = [k for k in install_requires if 'mpi4py' not in k]

#extensions = [
#    Extension(
#        "balsam.service.pack._packer",
#        ["balsam/service/pack/_packer.pyx"]
#    ),
#]

setup(
    install_requires = install_requires,
    #ext_modules = extensions,
)
