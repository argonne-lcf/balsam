from setuptools import setup, Extension
from os import path

extensions = [
    Extension(
        "balsam.service.pack._packer",
        ["balsam/service/pack/_packer.pyx"]
    ),
]

#here = path.abspath(path.dirname(__file__))
#version_path = path.join(here, 'balsam', '__version__.py')
#version_dict = {}
#with open(version_path) as f: 
#    exec(f.read(), version_dict)

setup(
    #version=version_dict['__version__'],
    ext_modules = extensions,
)
