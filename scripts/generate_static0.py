#!/usr/bin/env python3

import os
import sys
import numpy as np
import pandas as pd

def get_data(path):   
    treelets = pd.read_csv(os.path.join(path, "treelets.csv"))
    summary = pd.read_csv(os.path.join(path, "summary.csv"))

    num_rays = (treelets.sort_values('treeletId')
                        .groupby('treeletId')
                        .agg({'raysDequeued': 'sum'})
                        .raysDequeued
                        .to_numpy())

    nodes_per_ray = (summary.sort_values('treeletId')
                      .groupby('treeletId')
                      .agg({'trace': 'sum',
                            'visited': 'sum'}))
    
    nodes_per_ray = (nodes_per_ray.visited / nodes_per_ray.trace).to_numpy()
    return num_rays, nodes_per_ray

def generate_static0(num_rays, nodes_per_ray):
    average_ray_size = 250         # 500 bytes
    average_bandwidth = 50_000_000 # 25 MB/s receive

    cost_bw = average_ray_size / average_bandwidth
    cost_compute = 0.25e-6 # 0.5us per node visited
    
    weights = np.nan_to_num(np.maximum(cost_bw * num_rays,
                                       cost_compute * nodes_per_ray * num_rays))

    print(len(weights))

    for i in range(len(weights)):
        print(f"{weights[i]:.8f} {1} {i}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"{sys.argv[0]} logs-dir")
        sys.exit(1)

    num_rays, nodes_per_ray = get_data(sys.argv[1])
    generate_static0(num_rays, nodes_per_ray)
