import numpy as np
sizes = np.array([200])

for i in range(1):
    data = np.random.uniform(-1000.0, 1000.0, sizes[i]).astype(np.float64)
    data.tofile(f'bin{i}')


