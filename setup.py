'''A setuptools based setup module.

https://packaging.python.org/en/latest/distributing.html '''
from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop
from setuptools.extension import Extension
from codecs import open
from os import path
import os
import time

class PostInstallCommand(install):
    '''Post-installation for installation mode'''
    def run(self):
        install.run(self)
        from Cython.Build import cythonize
        extensions = [
            Extension("balsam.service.pack._packer", 
                      ["balsam/service/pack/_packer.pyx"])
        ]
        setup(ext_modules=cythonize(extensions))


class PostDevelopCommand(develop):
    '''Post-installation for installation mode'''
    def run(self):
        develop.run(self)
        from Cython.Build import cythonize
        extensions = [
            Extension("balsam.service.pack._packer", 
                      ["balsam/service/pack/_packer.pyx"])
        ]
        setup(ext_modules=cythonize(extensions))


here = path.abspath(path.dirname(__file__))
#activate_script = path.join(here, 'balsam', 'scripts', 'balsamactivate')
#deactivate_script = path.join(here, 'balsam', 'scripts', 'balsamdeactivate')
activate_script = path.join('balsam', 'scripts', 'balsamactivate')
deactivate_script = path.join('balsam', 'scripts', 'balsamdeactivate')
bcd_script = path.join('balsam', 'scripts', 'bcd')

ABOUT = {}
with open(path.join(here, 'balsam', '__version__.py')) as f:
    exec(f.read(), ABOUT)

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='balsam',
    version=ABOUT['__version__'],
    description='Auto Scheduler and HPC Job Manager',
    long_description=long_description,
    
    url='', # Home page
    author='Misha Salim',
    author_email='msalim@anl.gov',

    classifiers = [],

    keywords='',

    packages=find_packages(exclude=['docs','__pycache__','data','experiments','log',]),

    python_requires='>=3.6',

    install_requires=['cython', 'django==2.1.1', 'jinja2',
        'psycopg2-binary', 'sphinx', 'sphinx_rtd_theme', 'numpy'],

    package_data = {
        'balsam' : ['django_config/*.json',
                    'django_config/*.ini',
                    'django_config/job-templates/*.tmpl',
                   ],
    },

    # Command-line bash scripts (to be used as "source balsamactivate")
    scripts = [activate_script, deactivate_script, bcd_script],

    # Register command-line tools here
    entry_points={
        'console_scripts': [
            'balsam = balsam.scripts.cli:main',
            'balsam-test = run_tests:main'
        ],
        'gui_scripts': [],
    },

    # Balsam DB auto-setup post installation
    cmdclass={
        'develop': PostDevelopCommand,
        'install': PostInstallCommand,
    },
)
