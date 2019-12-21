#!/usr/bin/env python3

import pandas as pd
import numpy as np
import argparse
import os
import matplotlib
import matplotlib.pyplot as plt
import pint
import pylab

U = pint.UnitRegistry()
U.setup_matplotlib(True)

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', required=True)
parser.add_argument('-t', '--title', required=True)
parser.add_argument('-o', '--out', required=True)

args = parser.parse_args()

COLOR_MAP = pylab.cm.get_cmap('RdYlGn', 12)

def plot_heatmap(mat, title, xlabel, ylabel, out):
    plt.clf()

    plt.imshow(mat.transpose(), cmap=COLOR_MAP, interpolation='nearest',
               aspect='auto', extent=(0, mat.shape[0], mat.shape[1], 0),
               vmin=-1.5, vmax=mat.max(),
               norm=matplotlib.colors.PowerNorm(gamma=0.2))

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.clim(0, mat.max())
    plt.colorbar()
    plt.savefig(out, dpi=300)


def gen_per_second_per_treelet(df, out):
    per_second_per_treelet = df.groupby(['timestampS', 'treeletID']).sum()

    raysProcessed_ps_pt = per_second_per_treelet.raysProcessed
    timestamp_sums = raysProcessed_ps_pt.sum(level=0).to_numpy()
    mat = raysProcessed_ps_pt.unstack().to_numpy()
    mat = mat / timestamp_sums.reshape(-1, 1)
    mat = np.nan_to_num(mat)

    plot_heatmap(mat, args.title, "Time (s)", "Treelet ID", out)

def gen_per_second_per_worker(df, out):
    per_second_per_worker = df.groupby(['timestampS', 'workerID']).sum()

    raysProcessed_ps_pw = per_second_per_worker.raysProcessed
    timestamp_sums = raysProcessed_ps_pw.sum(level=0).to_numpy()
    mat = raysProcessed_ps_pw.unstack().to_numpy()
    mat = mat / timestamp_sums.reshape(-1, 1)
    mat = np.nan_to_num(mat)

    plot_heatmap(mat, args.title, "Time (s)", "Worker ID", out)

def gen_ray_queue(df, out, aggregate):
    if aggregate:
        per_time = df.groupby(['timestampS']).sum()
        plt.plot((per_time.raysWaiting - per_time.raysProcessed).cumsum())
    else:
        for id, group in df.groupby('workerID'):
            per_time = group.groupby(['timestampS']).sum()
            cumulative = (per_time.raysWaiting - per_time.raysProcessed).cumsum()
            plt.plot(cumulative, label=str(id))

        plt.legend()

    plt.title(args.title)
    plt.xlabel("Time (seconds)")
    plt.ylabel("Total Number of Waiting Rays")
    plt.savefig(out, dpi=300)

def sent_bytes(df, out):
    per_second_per_treelet = df.groupby(['timestampS', 'workerID']).sum()

    bytessent_ps_pt = per_second_per_treelet.bytesSent
    mat = bytessent_ps_pt.unstack().to_numpy()
    mat = np.nan_to_num(mat)

    plot_heatmap(mat, args.title, "Time (s)", "Worker ID", out)

def received_bytes(df, out):
    per_second_per_treelet = df.groupby(['timestampS', 'workerID']).sum()

    bytessent_ps_pt = per_second_per_treelet.bytesReceived
    mat = bytessent_ps_pt.unstack().to_numpy()
    mat = np.nan_to_num(mat)

    plot_heatmap(mat, args.title, "Time (s)", "Worker ID", out)

data = pd.read_csv(os.path.join(args.input, 'data.csv'))

gen_per_second_per_treelet(data, os.path.join(args.out, "per-treelet.png"))
gen_per_second_per_worker(data, os.path.join(args.out, "per-worker.png"))
gen_ray_queue(data, os.path.join(args.out, "aggregate-ray-queue.png"), True)
gen_ray_queue(data, os.path.join(args.out, "individual-ray-queue.png"), False)
sent_bytes(data, os.path.join(args.out, "per-worker-outrate.png"))
received_bytes(data, os.path.join(args.out, "per-worker-inrate.png"))
