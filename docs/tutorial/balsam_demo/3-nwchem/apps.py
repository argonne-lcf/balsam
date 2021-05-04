#!/usr/bin/env python
# apps.py
'''Register the two Applications'''
import os
from balsam.core.models import ApplicationDefinition as App

if not App.objects.filter(name="nwchem-water").exists():
    nwchem = App(
        name = 'nwchem-water',
        executable = '/soft/applications/nwchem/6.8/bin/nwchem',
        preprocess = os.path.abspath('pre.py'),
        postprocess = os.path.abspath('post.py'),
        envscript = os.path.abspath('envs.sh'),
    )
    nwchem.save()

if not App.objects.filter(name="plot-pes").exists():
    plotter = App(
        name = 'plot-pes',
        executable = os.path.abspath('plot.py')
    )
    plotter.save()

