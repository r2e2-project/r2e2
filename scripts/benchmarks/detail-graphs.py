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
parser.add_argument('-x', '--num-exclude', default=0, type=int)

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

def plot_completion_marks(data):
    targets = [0.25, 0.50, 0.75, 0.9, 0.95, 0.99, 1.0]

    fot = (data.groupby('timestampS')
               .agg({'timestampS': 'max', 'pathsFinished': 'sum'}))
    timestamps = fot.timestampS.to_numpy()
    fractions = np.cumsum(fot.pathsFinished.to_numpy() * TIMESTEP)
    fractions /= np.max(fractions)

    T = []
    i = 0

    for target in targets:
        while fractions[i] < target:
            i += 1

        A = (fractions[i - 1], timestamps[i - 1])
        B = (fractions[i], timestamps[i])

        t = A[1] + ((A[1] - B[1]) / (A[0] - B[0])) * (target - A[0])
        T += [t]

    #plt.scatter(T, [1] * len(T), marker='|', c='red')

    #ax2 = plt.gca().twiny()
    #ax2.set_xticks(T)
    #ax2.set_xticklabels(["{:d}%".format(int(f * 100)) for f in targets])

    for i in range(len(T)):
        plt.axvline(T[i], linestyle=':', linewidth=0.5, color='#999999')
        plt.annotate("{}%".format(int(targets[i] * 100)), (T[i], 0.98),
                     xycoords=("data", "axes fraction"), textcoords="offset pixels",
                     xytext=(10, 0),
                     fontsize='x-small', color='#999999', ha='left', va='center_baseline',
                     rotation='vertical')

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
    plt.clf()

def combined_progress_rate(df, out):
    maxtime = df['timestampS'].max()
    data = df.groupby(['timestampS']).sum()
    enqueued_per_sec = data.raysEnqueued
    dequeued_per_sec = data.raysDequeued
    total_paths = data.pathsFinished.sum()
    running_completion = data.pathsFinished.cumsum();
    percent_complete = running_completion / total_paths * 100

    fig, (ax, cax) = plt.subplots(nrows=2, sharex=True, gridspec_kw={'hspace': 0, 'wspace': 0})
    ax.grid(b=True, linewidth=0.5, color='#F6F6F6')
    cax.grid(b=True, linewidth=0.5, color='#F6F6F6')
    ax.spines['top'].set_bounds(-33, maxtime + 33)
    ax.spines['bottom'].set_visible(False)
    cax.spines['top'].set_bounds(-33, maxtime + 33)
    cax.spines['bottom'].set_bounds(-33, maxtime + 33)

    ax.set_ylabel("Number of Rays", fontsize=9)
    ax.yaxis.set_major_formatter(matplotlib.ticker.EngFormatter(sep='', places=0))
    deq_line = ax.plot(dequeued_per_sec, label='Rays Dequeued', linewidth=0.7, color='tab:blue')
    enq_line = ax.plot(enqueued_per_sec, label='Rays Enqueued', linewidth=0.7, color='tab:orange')

    altax = ax.twinx()
    altax.set_ylabel('Avg. Bag Bytes', fontsize=9, labelpad=12)
    altax.yaxis.set_major_formatter(matplotlib.ticker.EngFormatter(sep='', places=0))

    bag_line = altax.plot(data.bytesEnqueued / data.bagsEnqueued, label='Avg. Bytes per Bag', linewidth=0.7, color='#76B7B2')

    completion_line = cax.plot(percent_complete, label='% Paths Finished', linewidth=0.7, color='#E25C5E')

    cax.set_xlabel("Timestamp (s)", fontsize=9)
    cax.set_ylabel("Percent Paths Finished", fontsize=9)
    cax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter())

    plt.margins(x=0.0126)
    ax.tick_params(axis='both', which='major', labelsize=7)
    altax.tick_params(axis='both', which='major', labelsize=7)
    cax.tick_params(axis='both', which='major', labelsize=7)

    plt.title(args.title, fontsize=11)

    lines = enq_line + deq_line + bag_line + completion_line
    labels = [l.get_label() for l in lines]
    squares = [matplotlib.patches.Rectangle((0, 0), 1, 1, facecolor=l.get_color()) for l in lines]
    plt.legend(squares, labels, loc='lower left', bbox_to_anchor=(1, -1), handlelength=1, handleheight=1, prop={'size': 8})

    fig.tight_layout()
    plt.savefig(out, dpi=300, bbox_inches='tight')
    plt.clf()


treelet_data = pd.read_csv(os.path.join(args.input, 'treelets.csv'))
worker_data = pd.read_csv(os.path.join(args.input, 'workers.csv'))

treelet_data['timestampS'] = (treelet_data.timestamp / 1000).astype('int32')
worker_data['timestampS'] = (worker_data.timestamp / 1000).astype('int32')

tracer_worker_data = worker_data[worker_data.workerId >= args.num_exclude]

TIMESTEP = get_timestep(worker_data)

gen_per_second_per_treelet(treelet_data, os.path.join(args.out, "per-treelet.png"))
gen_per_second_per_worker(worker_data, os.path.join(args.out, "per-worker.png"))
sent_bytes(worker_data, os.path.join(args.out, "per-worker-outrate.png"))
received_bytes(tracer_worker_data, os.path.join(args.out, "per-worker-inrate.png"))
gen_ray_queue(worker_data, os.path.join(args.out, "aggregate-ray-queue.png"), True)
gen_ray_queue(worker_data, os.path.join(args.out, "individual-ray-queue.png"), False)
ray_throughput_over_time(worker_data, os.path.join(args.out, "ray-throughput.png"))
combined_progress_rate(worker_data, os.path.join(args.out, "progress-rate.png"))
