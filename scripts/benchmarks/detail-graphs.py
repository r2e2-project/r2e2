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

COLOR_MAP = pylab.cm.get_cmap('Greens', 10)
TIMESTEP = 1

def get_timestep(data):
    q = data.timestampS.unique()
    return q[-1] - q[-2]

def x_fmt(x, y):
    global TIMESTEP
    return '{:}'.format(int(x) * TIMESTEP)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Y', suffix)

def plot_heatmap(mat, title, xlabel, ylabel, out):
    plt.imshow(mat.transpose(), cmap=COLOR_MAP, interpolation='nearest',
               aspect='auto', extent=(0, mat.shape[0], mat.shape[1], 0),
               vmin=-1.5, vmax=mat.max())

    plt.gca().xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(x_fmt))

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.clim(0, mat.max())
    plt.colorbar()
    plt.savefig(out, dpi=300)
    plt.clf()

def gen_per_second_per_treelet(df, out):
    per_second_per_treelet = df.groupby(['timestampS', 'treeletId']).sum()

    raysProcessed_ps_pt = per_second_per_treelet.raysDequeued
    timestamp_sums = raysProcessed_ps_pt.sum(level=0).to_numpy()
    mat = raysProcessed_ps_pt.unstack().to_numpy()
    mat = mat / timestamp_sums.reshape(-1, 1)
    mat = np.nan_to_num(mat)

    plot_heatmap(mat, args.title +
                      '\nRays Dequeued (per treelet)', "Time (s)", "Treelet ID", out)

def gen_per_second_per_worker(df, out):
    per_second_per_worker = df.groupby(['timestampS', 'workerId']).sum()

    raysProcessed_ps_pw = per_second_per_worker.raysDequeued
    timestamp_sums = raysProcessed_ps_pw.sum(level=0).to_numpy()
    mat = raysProcessed_ps_pw.unstack().to_numpy()
    mat = mat / timestamp_sums.reshape(-1, 1)
    mat = np.nan_to_num(mat)

    plot_heatmap(mat, args.title +
                      '\nRays Dequeued (per worker)', "Time (s)", "Worker ID", out)

def gen_ray_queue(df, out, aggregate):
    if aggregate:
        per_time = df.groupby(['timestampS']).sum()
        plt.plot((per_time.raysEnqueued - per_time.raysDequeued).cumsum())
    else:
        for id, group in df.groupby('workerId'):
            per_time = group.groupby(['timestampS']).sum()
            cumulative = (per_time.raysEnqueued - per_time.raysDequeued).cumsum()
            plt.plot(cumulative, label=str(id))

        plt.legend()

    plt.title(args.title)
    plt.xlabel("Time (s)")
    plt.ylabel("Total Number of Waiting Rays")
    plt.gcf().subplots_adjust(left=0.2)
    plt.savefig(out, dpi=300)
    plt.clf()

def sent_bytes(df, out):
    per_second_per_treelet = df.groupby(['timestampS', 'workerId']).sum()

    bytessent_ps_pt = per_second_per_treelet.bytesEnqueued
    mat = bytessent_ps_pt.unstack().to_numpy()
    mat = np.nan_to_num(mat)

    max_rate = np.max(mat)
    mat /= max_rate

    plot_heatmap(mat, args.title +
                      '\nSend Rate / Max Rate (Max = {}/s)'.format(sizeof_fmt(max_rate)),
                 "Time (s)",
                 "Worker ID", out)

def received_bytes(df, out):
    per_second_per_treelet = df.groupby(['timestampS', 'workerId']).sum()

    bytessent_ps_pt = per_second_per_treelet.bytesDequeued
    mat = bytessent_ps_pt.unstack().to_numpy()
    mat = np.nan_to_num(mat)

    max_rate = np.max(mat)
    mat /= max_rate

    plot_heatmap(mat, args.title +
                      '\nRecv Rate / Max Rate (Max = {}/s)'.format(sizeof_fmt(max_rate)),
                 "Time (s)",
                 "Worker ID", out)

def ray_throughput_over_time(df, out):
    mat = df.groupby(['timestampS']).sum().numSamples

    plt.plot(mat)
    plt.title(args.title + '\nRay throughput over time')
    plt.xlabel("Time (s)")
    plt.ylabel("Throughput (rays / worker / s)")
    plt.gcf().subplots_adjust(left=0.2)
    plt.savefig(out, dpi=300)

treelet_data = pd.read_csv(os.path.join(args.input, 'treelets.csv'))
worker_data = pd.read_csv(os.path.join(args.input, 'workers.csv'))

treelet_data['timestampS'] = (treelet_data.timestamp / 1000).astype('int32')
worker_data['timestampS'] = (worker_data.timestamp / 1000).astype('int32')

TIMESTEP = get_timestep(worker_data)

gen_per_second_per_treelet(treelet_data, os.path.join(args.out, "per-treelet.png"))
gen_per_second_per_worker(worker_data, os.path.join(args.out, "per-worker.png"))
sent_bytes(worker_data, os.path.join(args.out, "per-worker-outrate.png"))
received_bytes(worker_data, os.path.join(args.out, "per-worker-inrate.png"))
gen_ray_queue(worker_data, os.path.join(args.out, "aggregate-ray-queue.png"), True)
gen_ray_queue(worker_data, os.path.join(args.out, "individual-ray-queue.png"), False)
ray_throughput_over_time(worker_data, os.path.join(args.out, "ray-throughput.png"))
