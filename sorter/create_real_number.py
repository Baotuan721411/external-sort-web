import numpy as np
sizes = np.array([500, 2000, 5000000])

for i in range(3):
    data = np.random.uniform(-1000.0, 1000.0, sizes[i]).astype(np.float64)
    data.tofile(f'bin{i}')


