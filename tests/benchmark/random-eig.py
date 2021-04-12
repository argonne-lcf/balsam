import sys

import numpy as np
from numpy.linalg import eigh

# result_file = Path.cwd().with_suffix(".eigs.npy").name  # used for # batch-cluster experiment naming
result_file = "result.npy"  # used by balsam eig.Eig App stage out
input_file = sys.argv[1]
data = np.load(input_file)
print(data.nbytes / 1e9, "GB loaded")
eigvals, eigvecs = eigh(data)
np.save(result_file, eigvals)
