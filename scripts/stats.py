#!/usr/bin/env python3

import os
import sys
import pandas as pd

def get_stats(stats_file):
    data = pd.read_csv(stats_file)
    data['timestamp_s'] = (data.timestamp - min(data.timestamp)) / 1e6
    data = data.sort_values(by='timestamp')

    worker_count = data.workerID.nunique()

    data = (data.groupby('timestamp_s')
                .agg({'timestamp_s': 'min',
                      'raysGenerated': 'sum',
                      'raysFinished': 'sum'})
                .reset_index(drop=True))

    data = data[(data.raysFinished != 0) | (data.raysGenerated != 0)]
    duration = max(data.timestamp_s) - min(data.timestamp_s)
    total_rays = int(data.agg({'raysFinished': 'sum'})['raysFinished'])
    max_rays = int(data.agg({'raysFinished': 'max'})['raysFinished'])

    print(f'Average: {total_rays / duration / worker_count:.2f} rays/core/s')
    print(f'   Peak: {max_rays / 2 / worker_count:.2f} rays/core/s')

def main():
    get_stats(sys.argv[1])

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} FILENAME")
        exit(1)

    main()
