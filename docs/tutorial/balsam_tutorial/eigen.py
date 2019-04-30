'''Get eigenvalues'''
import sys
import numpy as np

mat_file = sys.argv[1]
matrix = np.load(mat_file)
eigvals = np.linalg.eigvalsh(matrix)
np.save("eigvals", eigvals)
