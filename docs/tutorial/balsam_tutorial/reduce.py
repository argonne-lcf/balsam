'''Read and sort many eigenvalues'''
import glob
import numpy as np

eig_files = glob.glob("eigvals*")
eigs = [np.load(f) for f in eig_files]
eigs = np.concatenate(eigs)
eigs.sort()

lo5 = '\n'.join(str(x) for x in eigs[:5])
hi5 = '\n'.join(str(x) for x in eigs[-5:])

with open("results.dat", 'w') as fp:
    fp.write("Lowest 5:\n")
    fp.write(lo5)
    fp.write("\nHighest 5:\n")
    fp.write(hi5)
