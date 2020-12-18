import numpy as np
import pandas as pd

columns = list(map(lambda n: f'column{n}', range(100)))
print('generating data')

data = {}
sample_arr = [True, False]
num_rows = 500000
num_cols = 100

for i in range(25):
    offset = i*4
    data = {
        **data,
        f'columns{offset}': np.random.randn(num_rows), #float
        f'columns{offset+1}': np.random.randint(-100000, 100000, size=num_rows), #int
        f'columns{offset+2}': np.random.choice(sample_arr, size=num_rows), #bool
        f'columns{offset+3}': pd.util.testing.rands_array(10, num_rows), #string
    }


df = pd.DataFrame(data)
print('writing to files')
df.to_csv('data_all.csv', index=None)

## generate all float dataset
df = pd.DataFrame(np.random.randn(num_rows, num_cols))
df.to_csv('data_all_float.csv', index=None)

## generate all int dataset
df = pd.DataFrame(np.random.randint(-100000, 100000, size=(num_rows, num_cols)))
df.to_csv('data_all_int.csv', index=None)

## generate all bool dataset
df = pd.DataFrame(np.random.choice(sample_arr, size=(num_rows, num_cols)))
df.to_csv('data_all_bool.csv', index=None)

## generate all string dataset
df = pd.DataFrame(pd.util.testing.rands_array(10, (num_rows, num_cols)))
df.to_csv('data_all_string.csv', index=None)