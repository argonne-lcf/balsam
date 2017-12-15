'''A setuptools based setup module.

https://packaging.python.org/en/latest/distributing.html
'''

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='balsam',
    version='0.1',
    description='HPC Edge service and workflow management',
    long_description=long_description,
    
    url='', # Home page
    author='',
    author_email='',

    classifiers = [],

    keywords='',

    packages=find_packages(exclude=['docs','__pycache__','data','experiments','log','tests']),

    install_requires=['django', 'django-concurrency'],

    include_package_data=True,

    # Register command-line tools here
    entry_points={
        'console_scripts': [
            'balsam = balsam.scripts.cli:main',
            'balsam-test = run_tests:main'
        ],
        'gui_scripts': [],
    },
)
