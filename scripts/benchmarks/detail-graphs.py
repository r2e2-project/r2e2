#!/usr/bin/env python3

import pandas as pd
import numpy as np
import argparse
import os
import matplotlib
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser()
parser.add_argument('csv')
parser.add_argument('-t', '--title', required=True)
parser.add_argument('-o', '--out', required=True)
 
args = parser.parse_args()

data = pd.read_csv(args.csv)

def gen_per_second_per_treelet(df, out):
    per_second_per_treelet = df.groupby(['timestampS', 'treeletID']).sum()
    
    raysProcessed_ps_pt = per_second_per_treelet.raysProcessed
    timestamp_sums = raysProcessed_ps_pt.sum(level=0).to_numpy()
    mat = raysProcessed_ps_pt.unstack().to_numpy()
    mat = mat / timestamp_sums.reshape(-1, 1)
    mat = np.nan_to_num(mat)
    
    plt.imshow(mat.transpose(), cmap='OrRd', interpolation='nearest', aspect='auto', extent=(0, mat.shape[0], mat.shape[1], 0), vmin=-1.5, vmax=mat.max(), norm=matplotlib.colors.PowerNorm(gamma=0.2))
    plt.xlabel("Time (seconds)")
    plt.ylabel("Treelet ID")
    plt.title(args.title)
    plt.clim(0, mat.max())
    plt.colorbar()
    plt.savefig(out, dpi=300)


def gen_per_second_per_worker(df, out):
    per_second_per_worker = df.groupby(['timestampS', 'workerID']).sum()
    
    raysProcessed_ps_pw = per_second_per_worker.raysProcessed
    timestamp_sums = raysProcessed_ps_pw.sum(level=0).to_numpy()
    mat = raysProcessed_ps_pw.unstack().to_numpy()
    mat = mat / timestamp_sums.reshape(-1, 1)
    mat = np.nan_to_num(mat)

    plt.imshow(mat.transpose(), cmap='OrRd', interpolation='nearest', aspect='auto', extent=(0, mat.shape[0], mat.shape[1], 0), vmin=-1.5, vmax=mat.max(), norm=matplotlib.colors.PowerNorm(gamma=0.2))
    plt.xlabel("Time (seconds)")
    plt.ylabel("Worker ID")
    plt.title(args.title)
    plt.clim(0, mat.max())
    plt.colorbar()
    plt.savefig(out, dpi=300)

def gen_ray_queue(df, out, aggregate):
    if aggregate:
        per_time = df.groupby(['timestampS']).sum()
        plt.plot((per_time.raysWaiting - per_time.raysProcessed).cumsum())
    else:
        for id, group in df.groupby('workerID'):
            per_time = group.groupby(['timestampS']).sum()
            cumulative = (per_time.raysWaiting - per_time.raysProcessed).cumsum()
            if cumulative.max() > 100:
                print(id)
            plt.plot(cumulative, label=str(id))

        plt.legend()

    plt.title(args.title)
    plt.xlabel("Time (seconds)")
    plt.ylabel("Total Number of Waiting Rays")
    plt.savefig(out, dpi=300)

gen_per_second_per_treelet(data, os.path.join(args.out, "per-treelet.png"))
plt.clf()
gen_per_second_per_worker(data, os.path.join(args.out, "per-worker.png"))
plt.clf()
gen_ray_queue(data, os.path.join(args.out, "aggregate-ray-queue.png"), aggregate=True)
plt.clf()
gen_ray_queue(data, os.path.join(args.out, "individual-ray-queue.png"), aggregate=False)
