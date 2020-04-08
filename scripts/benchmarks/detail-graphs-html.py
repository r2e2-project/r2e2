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

def pseudo_eng(divisor, suffix):
    def formatter(val, tick):
        trunc = val / divisor
        return '{0:g}'.format(trunc) + suffix

    return formatter

def combined_progress_rate(df, out):
    maxtime = df['timestampS'].max()
    data = df.groupby(['timestampS']).sum()
    data['runningCompletion'] = data.pathsFinished.cumsum();
    finish_point = data.runningCompletion.idxmax()
    data = data.loc[data.index <= finish_point]

    enqueued_per_sec = data.raysEnqueued
    dequeued_per_sec = data.raysDequeued
    total_paths = data.pathsFinished.sum()
    percent_complete = data.runningCompletion / total_paths * 100

    fig, (ax, cax) = plt.subplots(nrows=2, sharex=True, gridspec_kw={'hspace': 0, 'wspace': 0})
    ax.grid(b=True, linewidth=0.5, color='#F6F6F6')
    cax.grid(b=True, linewidth=0.5, color='#F6F6F6')
    ax.spines['bottom'].set_visible(False)
    overshoot = maxtime / 7
    ax.spines['top'].set_bounds(-overshoot, maxtime + overshoot)
    cax.spines['top'].set_bounds(-overshoot, maxtime + overshoot)
    cax.spines['bottom'].set_bounds(-overshoot, maxtime + overshoot)

    ax.set_ylabel("Number of Rays", fontsize=8)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(pseudo_eng(1e6, 'M')))
    altax = ax.twinx()
    altax.set_ylabel('Avg. Bag Bytes', fontsize=8, labelpad=12)

    altax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(pseudo_eng(1e3, 'K')))

    cax.set_xlabel("Timestamp (s)", fontsize=8)
    cax.set_ylabel("Percent Paths Finished", fontsize=8)
    cax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter())

    caltax = cax.twinx()
    caltax.set_ylabel("Bytes Transferred", fontsize=8)
    caltax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(pseudo_eng(1e6, 'M')))

    plt.margins(x=0.0126)
    ax.tick_params(axis='both', which='major', labelsize=7)
    altax.tick_params(axis='both', which='major', labelsize=7)
    cax.tick_params(axis='both', which='major', labelsize=7)
    caltax.tick_params(axis='both', which='major', labelsize=7)


    enq_line = ax.plot(enqueued_per_sec, label='Rays Enqueued', linewidth=0.7, color='tab:orange')
    deq_line = ax.plot(dequeued_per_sec, label='Rays Dequeued', linewidth=0.7, color='tab:blue')
    bag_line = altax.plot(data.bytesEnqueued / data.bagsEnqueued, label='Avg. Bytes per Bag', linewidth=0.7, color='#76B7B2')
    completion_line = cax.plot(percent_complete, label='% Paths Finished', linewidth=0.7, color='#E25C5E')
    bw_line = caltax.plot(data.bytesEnqueued + data.bytesDequeued, label='Network Bandwidth', linewidth=0.7, color='tab:green')

    lines = enq_line + deq_line + bag_line + completion_line + bw_line
    labels = [l.get_label() for l in lines]
    squares = [matplotlib.patches.Rectangle((0, 0), 1, 1, facecolor=l.get_color()) for l in lines]
    fig.legend(squares, labels, loc='upper left', bbox_to_anchor=(0.96, 1), handlelength=1, handleheight=1, prop={'size': 8})
    fig.suptitle(args.title, fontsize=11, y=1.01)

    fig.tight_layout()
    import mpld3
    print(mpld3.fig_to_dict(fig))
    plt.savefig(out, dpi=300, bbox_inches='tight')
    plt.clf()

treelet_data = pd.read_csv(os.path.join(args.input, 'treelets.csv'))
worker_data = pd.read_csv(os.path.join(args.input, 'workers.csv'))

treelet_data['timestampS'] = (treelet_data.timestamp / 1000).astype('int32')
worker_data['timestampS'] = (worker_data.timestamp / 1000).astype('int32')
worker_data['totalTransferred'] = worker_data.bytesEnqueued + worker_data.bytesDequeued

combined_progress_rate(worker_data, os.path.join(args.out, "progress-rate.png"))
