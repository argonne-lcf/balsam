#!/usr/bin/env python
# pre.py 
'''Generate input file for NWChem'''

import numpy as np 
from balsam.launcher.dag import current_job

def water_cartesian(r, theta):
    cost = np.cos(np.radians(theta))
    sint = np.sin(np.radians(theta))
    O = ['O', 0.0, 0.0, 0.0]
    H1 = ['H', r, 0.0, 0.0]
    H2 = ['H', r*cost, r*sint, 0.0]
    return (O,H1,H2)

def input_deck(cartesian_coords):
    coords = [' '.join(map(str, c)) for c in cartesian_coords]
    return f'''
    start h2o
    title "Water in 6-31g basis"

    geometry
    {coords[0]}
    {coords[1]}
    {coords[2]}
    end
    basis
    * library 6-31g
    end
    task scf
    '''

data = current_job.data
r, theta = data['r'], data['theta']
coords = water_cartesian(r, theta)
with open('input.nw', 'w') as fp:
    fp.write(input_deck(coords))

