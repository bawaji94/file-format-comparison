import os
import argparse
import datatable as dt
import pandas as pd
import numpy as np
import time

def arg_parser():
    parser = argparse.ArgumentParser(description='benchmark table data file formats')
    parser.add_argument('--benchmark-name', help='', required=True)
    return parser.parse_args()

def convert_to_MB(size):
    return size/(1024*1024)

def convert_to_KB(size):
    return size/1024

def file_name(f):
    return f'./files/data.{f}'

def benchmark_data(file_format, write_file, read_file, library, engine):
    write_times = []
    read_times= []
    number_of_runs = 10

    for run in range(number_of_runs):
        print(f'{file_format}_{run}/{number_of_runs}', end='\r')
        write_time_start = time.time()
        write_file()
        write_time_end = time.time()
        write_times.append(write_time_end - write_time_start)

        read_time_start = time.time()
        read_file()
        read_time_end = time.time()
        read_times.append(read_time_end - read_time_start)
        file_size_in_bytes = os.path.getsize(file_name(file_format))
        os.remove(file_name(file_format))


    file_size = round(convert_to_MB(file_size_in_bytes), 2)
    b = {
        'fileFormat': file_format,
        'library': library,
        'write_time': round(np.mean(write_times), 4),
        'read_time': round(np.mean(read_times), 4),
        'sizeOnDisk': file_size,
        'engine': engine
    }
    print(b)
    return b

    


if __name__ == '__main__':
    if not os.path.exists('files'):
        os.makedirs('files')
    args = arg_parser()
    benchmark_name = args.benchmark_name
    DT_df = dt.fread(f'source/{benchmark_name}.csv')
    pd_df = DT_df.to_pandas()

    formats = [
        {
            'format': 'feather',
            'write': lambda: pd_df.to_feather(file_name('feather')),
            'read': lambda: pd.read_feather(file_name('feather')),
            'library': 'pandas'
        },
        {
            'format': 'parquet',
            'write': lambda: pd_df.to_parquet(file_name('parquet'), index=None, compression=None, engine='pyarrow'),
            'read': lambda: pd.read_parquet(file_name('parquet'), engine='pyarrow'),
            'library': 'pandas',
            'engine': 'pyarrow'
        },
        {
            'format': 'parquet.snappy',
            'write': lambda: pd_df.to_parquet(file_name('parquet.snappy'), index=None, compression='snappy', engine='pyarrow'),
            'read': lambda: pd.read_parquet(file_name('parquet.snappy'), engine='pyarrow'),
            'library': 'pandas',
            'engine': 'pyarrow'
        },
        {
            'format': 'parquet.gzip',
            'write': lambda: pd_df.to_parquet(file_name('parquet.gzip'), index=None, compression='gzip', engine='pyarrow'),
            'read': lambda: pd.read_parquet(file_name('parquet.gzip'), engine='pyarrow'),
            'library': 'pandas',
            'engine': 'pyarrow'
        },
        {
            'format': 'parquet.brotli',
            'write': lambda: pd_df.to_parquet(file_name('parquet.brotli'), index=None, compression='brotli', engine='pyarrow'),
            'read': lambda: pd.read_parquet(file_name('parquet.brotli'), engine='pyarrow'),
            'library': 'pandas',
            'engine': 'pyarrow'
        },
        {
            'format': 'csv',
            'write': lambda: DT_df.to_csv(file_name('csv')),
            'read': lambda: dt.fread(file_name('csv')),
            'library': 'Datatables'
        },
        {
            'format': 'jay',
            'write': lambda: DT_df.to_jay(file_name('jay')),
            'read': lambda: dt.fread(file_name('jay')),
            'library': 'Datatables'
        }
    ]

    benchmarks = []
    for format in formats:
        benchmark = benchmark_data(
            format['format'], 
            format['write'], 
            format['read'], 
            format['library'],
            format['engine'] if 'engine' in format else ''
        )
        benchmarks.append(benchmark)

    pd.DataFrame(benchmarks).to_csv(f'benchmarks/{benchmark_name}_file_format_benchmark.csv', index=None)
    os.rmdir('files')
