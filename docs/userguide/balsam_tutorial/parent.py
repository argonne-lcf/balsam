'''Generate random matrices'''
import random
import numpy as np

num_outputs = random.randint(2,6)

for i in range(num_outputs):

    out_path = f"output{i}.npy"
    dim = random.randint(10,100)
    data = np.random.random((dim, dim))
    data = 0.5*(data + data.T)
    np.save(out_path, data)
