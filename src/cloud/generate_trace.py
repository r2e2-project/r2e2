import sys
import struct
import json
import random
import tarfile
import os
import argparse
import pint
import math

U = pint.UnitRegistry()

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.rcParams['image.interpolation'] = 'nearest'
import seaborn as sns
import numpy as np
import pandas as pd

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from matplotlib.colors import LogNorm
from PIL import Image

sns.set_style("ticks", {'axes.grid': True})

class WorkerStats(object):
    def __init__(self, file_path : str):
        stats = self._read_stats(file_path)
        self.intervals = stats['intervals']
        self.metrics = stats['metrics']

    def _read_intervals(self, f, num_workers):
        worker_intervals = {}
        for line in f:
            tokens = line.split(' ')
            worker_id = int(tokens[1])
            num_actions = int(tokens[2])
            offset = 3
            intervals = []
            for _ in range(num_actions):
                action_name = tokens[offset]
                num_intervals = int(tokens[offset + 1])
                offset += 2
                for n in range(num_intervals):
                    start, end = tokens[offset + n].split(',')
                    intervals.append((action_name, int(start), int(end)))
                offset += num_intervals
            worker_intervals[worker_id] = intervals
            if len(worker_intervals) == num_workers:
                break
        return worker_intervals
    
    def _read_metrics(self, f, num_workers):
        worker_metrics = defaultdict(dict)
        for line in f:
            tokens = line.split(' ')
            worker_id = int(tokens[1])
            num_metrics = int(tokens[2])
            offset = 3
            for _ in range(num_metrics):
                metrics = []
                metric_name = tokens[offset]
                num_points = int(tokens[offset + 1])
                offset += 2
                for n in range(num_points):
                    time, value = tokens[offset + n].split(',')
                    metrics.append((int(time), float(value)))
                offset += num_points
                worker_metrics[worker_id][metric_name] = metrics
            if len(worker_metrics) == num_workers:
                break
        return worker_metrics
    
    def _read_stats(self, path: str):
        stats = {}
        with open(path, 'r') as f:
            num_workers = int(next(f))
            for line in f:
                line = line.strip()
                if line == 'intervals':
                    stats['intervals'] = self._read_intervals(f, num_workers)
                elif line == 'metrics':
                    stats['metrics'] = self._read_metrics(f, num_workers)
                else:
                    print('Unrecognized stats header "{:s}"'.format(line))
                    exit(-1)
        return stats


class Constants(object):
     # Ray data structure size
     INT_SIZE = 4 * U.bytes
     FLOAT_SIZE = 4 * U.bytes
     STACK_LENGTH = 20
     RAY_FIELDS = {
         'origin': 2 * 3 * FLOAT_SIZE,
         'sample_location': 2 * FLOAT_SIZE,
         'min_t': FLOAT_SIZE,
         'max_t': FLOAT_SIZE,
         'weight': FLOAT_SIZE,
         'medium': INT_SIZE,
         'stack': INT_SIZE * STACK_LENGTH
         }
     RADIANCE_RAY_FIELDS = {
         'origin_x': 3 * FLOAT_SIZE,
         'direction_x': 3 * FLOAT_SIZE,
         'origin_y': 3 * FLOAT_SIZE,
         'direction_y': 3 * FLOAT_SIZE
         }
     SHADOW_RAY_SIZE = sum([x for _, x in RAY_FIELDS.items()])
     RADIANCE_RAY_SIZE = SHADOW_RAY_SIZE + sum(
         [x for _, x in RADIANCE_RAY_FIELDS.items()])
     
     
     # Global constants
     W = 1920 # image width
     H = 1080 # image height
     SAMPLES_PER_PIXEL = 256 # samples per pixel
     GEOMETRY_SIZE = 20 * U.gigabytes # size of the scene geometry
     TS = 1 * U.gigabytes # treelet size
     TT = GEOMETRY_SIZE / TS # total treelets
     
     S = W * H * SAMPLES_PER_PIXEL # samples per pixel
     L = 5 # average path length (# of bounces)
     T = np.log(TT) # average number of treelets per ray
     RAY_FOOTPRINT = SHADOW_RAY_SIZE * 0.5 + RADIANCE_RAY_SIZE * 0.5  # average ray footprint
     TOTAL_FOOTPRINT = (RAY_FOOTPRINT * S * (2 * L - 1)).to(U.gigabyte)

     LAMBDA_BANDWIDTH = 47 * U.gigabytes
     LAMBDA_BOOT_TIME = 4
     LAMBDA_COST = 0.18 / (60 * 60)

     CPU_TIME_PER_RAY_TRAVERSAL = 40 * 1e-6


class SceneStats(object):
    def __init__(self, scene_file_path):
        self.geometry_size = 10
        self.treelet_size = 10
        self.num_treelets = 10
        self.image_width = 1920
        self.image_height = 1020
        self.samples_per_pixel = 256
        self.num_lambdas = 600

        self.shadow_ray_size = Constants.SHADOW_RAY_SIZE
        self.radiance_ray_size = Constants.RADIANCE_RAY_SIZE
        self.ray_size = (self.shadow_ray_size + self.radiance_ray_size) / 2

        self.total_samples = self.image_width * self.image_height * self.samples_per_pixel
        self.path_length = Constants.L
        self.traversals_per_ray = np.log(self.num_treelets)


def write_trace(stats, path: str):
    """
    Generates a trace file in Chrome format.

    To visualize the trace, visit chrome://tracing in Google Chrome and
    click "Load" in the top left to load the trace.

    Args
    ----
    path
      Output path to write the trace.
    """

    worker_intervals = stats.intervals

    # https://github.com/catapult-project/catapult/blob/master/tracing/tracing/base/color_scheme.html
    colors = {'idle': 'grey'}
    traces = []

    def make_trace_from_interval(interval, proc, tid):
        name, start, end = interval
        cat = ''
        trace = {
            'name': name,
            'cat': cat,
            'ph': 'X',
            'ts': start / 1000,  # ns to microseconds
            'dur': (end - start) / 1000,
            'pid': proc,
            'tid': tid,
            'args': {}
        }
        if interval[0] in colors:
            trace['cname'] = colors[interval[0]]
        return trace


    if False and self._master_profiler is not None:
        traces.append({
            'name': 'thread_name',
            'ph': 'M',
            'pid': -1,
            'tid': 0,
            'args': {'name': 'master'}
        })

        for interval in self._master_profiler[1]['intervals']:
            traces.append(make_trace_from_interval(interval, 'master', -1, 0))

    for worker_id, intervals in worker_intervals.items():
        tid = worker_id
        worker_num = worker_id
        tag = None
        proc = worker_id

        print('Generating traces for worker {:d}...'.format(tid))

        traces.append({
            'name': 'thread_name',
            'ph': 'M',
            'pid': proc,
            'tid': tid,
            'args': {
                'name':
                '{:06d}'.format(worker_num) +
                ("_" + str(tag) if tag else "")
            }
        })
        for interval in intervals:
            if interval[0] == 'idle':
                # We ignore the idle interval when visualizing
                continue
            traces.append(make_trace_from_interval(interval, proc, tid))


    worker_metrics = stats.metrics

    def make_counter_from_metric(name, metric, proc, tid):
        time, value = metric
        cat = ''
        trace = {
            'name': name,
            'cat': cat,
            'ph': 'C',
            'ts': time / 1000,  # ns to microseconds
            'pid': proc,
            'tid': tid,
            'args': {
                name: value
            }
        }
        if metric[0] in colors:
            trace['cname'] = colors[interval[0]]
        return trace


    for worker_id, metrics in worker_metrics.items():
        tid = worker_id
        worker_num = worker_id
        tag = None
        proc = worker_id

        print('Generating metrics for worker {:d}...'.format(tid))

        for name, points in metrics.items():
            for point in points:
                traces.append(make_counter_from_metric(name, point, proc, tid))


    parts = path.split('.')
    base = parts[0]
    exts = parts[1:]
    with open(base + '.trace', 'w') as f:
        f.write(json.dumps(traces))

    if exts == ['trace']:
        return path

    elif exts == ['tar', 'gz']:
        with tarfile.open(base + '.tar.gz', 'w:gz') as tar:
            tar.add(base + '.trace')
        os.remove(base + '.trace')
        return path

    else:
        raise Exception("Invalid trace extension '{}'. Must be .trace or .tar.gz." \
                        .format(''.join(['.' + e for e in exts])))

def quantize_sequence(sequence, quantization):
    quantized_sequence = []

    if len(sequence) == 0:
        return []

    current_time = sequence[0][0]
    offset = 1
    summed_value = 0
    while offset < len(sequence):
        timePrev, valuePrev = sequence[offset - 1]
        time, value = sequence[offset]
    
        interval = float(time - timePrev)
        contribution = value / interval
        #print(metric_name, value, time, timePrev, interval)
        if time >= current_time:
            # Add % contribution from prior interval
            alpha = 1.0 - (time - current_time) / interval
            beta = 1.0 - alpha
            #print(time, current_time, alpha)
            summed_value += contribution * alpha
            quantized_sequence.append((current_time, summed_value))

            current_time += quantization
            summed_value = 0
    
            summed_value += contribution * beta
        else:
            summed_value += contribution
        offset += 1
    return quantized_sequence


def heatmap(data, row_labels, col_labels, ax=None,
            cbar_kw={}, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Arguments:
        data       : A 2D numpy array of shape (N,M)
        row_labels : A list or array of length N with the labels
                     for the rows
        col_labels : A list or array of length M with the labels
                     for the columns
    Optional arguments:
        ax         : A matplotlib.axes.Axes instance to which the heatmap
                     is plotted. If not provided, use current axes or
                     create a new one.
        cbar_kw    : A dictionary with arguments to
                     :meth:`matplotlib.Figure.colorbar`.
        cbarlabel  : The label for the colorbar
    All other arguments are directly passed on to the imshow call.
    """

    if not ax:
        ax = plt.gca()

    # Plot the heatmap
    im = ax.imshow(data + 1.0, **kwargs)

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")

    # We want to show all ticks...
    #ax.set_xticks(np.arange(data.shape[1]))
    #ax.set_yticks(np.arange(data.shape[0]))
    # ... and label them with the respective list entries.
    ax.set_xlim([min(col_labels), max(col_labels)])
    #ax.set_xticks(np.arange(data.shape[1]))
    #ax.set_xticklabels(col_labels)
    #ax.set_yticklabels(row_labels)

    # Let the horizontal axes labeling appear on top.
    #ax.tick_params(top=True, bottom=False,
    #               labeltop=True, labelbottom=False)

    # Rotate the tick labels and set their alignment.
    #plt.setp(ax.get_xticklabels(), rotation=-30, ha="right",
    #         rotation_mode="anchor")

    # Turn spines off and create white grid.
    for edge, spine in ax.spines.items():
        spine.set_visible(False)

    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=0.1)
    ax.grid(which="major", color="w", linestyle='-', linewidth=0)
    ax.tick_params(which="minor", bottom=False, left=False)

    return im, cbar


def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                     textcolors=["black", "white"],
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Arguments:
        im         : The AxesImage to be labeled.
    Optional arguments:
        data       : Data used to annotate. If None, the image's data is used.
        valfmt     : The format of the annotations inside the heatmap.
                     This should either use the string format method, e.g.
                     "$ {x:.2f}", or be a :class:`matplotlib.ticker.Formatter`.
        textcolors : A list or array of two color specifications. The first is
                     used for values below a threshold, the second for those
                     above.
        threshold  : Value in data units according to which the colors from
                     textcolors are applied. If None (the default) uses the
                     middle of the colormap as separation.

    Further arguments are passed on to the created text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[im.norm(data[i, j]) > threshold])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)

    return texts


def plot_time_heatmap(stats, metric_label):
    quantization = 1000 * 1e6 # ms to nanoseconds

    # Deteremine number of rows
    num_timepoints = 0
    for worker_id, metrics in stats.metrics.items():
        points = metrics[metric_label]
        for time, _ in points:
            quantized_time = time // quantization 
            if quantized_time > num_timepoints:
                num_timepoints = quantized_time
    num_timepoints = int(num_timepoints) + 1

    # Deteremine number of columns
    num_workers = len(stats.metrics)
    
    data = np.zeros((num_workers, num_timepoints))
    row_labels = []
    for worker_idx, (worker_id, metrics) in enumerate(stats.metrics.items()):
        row_labels.append(worker_id)
        points = metrics[metric_label]
        quantized_points = quantize_sequence(points, quantization)
        for i, (time, value) in enumerate(quantized_points):
            data[worker_idx, i] = value * 1e9
    col_labels = [int(x[0] * 1e-9) for x in quantized_points]
    fig = plt.figure(dpi=300)
    ax = plt.subplot()
    im, cbar = heatmap(data, row_labels, col_labels, ax=ax,
                       norm=LogNorm())
    return fig


def plot_metrics(stats, path):
    quantization = 1000 * 1e6 # nanoseconds

    plt.clf()
    metric_labels = ['udpBytesSent', 'udpBytesReceived', 'cpuTime', 'udpOutstanding']
    # Plot heatmaps for each worker over time
    for metric_name in metric_labels:
        fig = plot_time_heatmap(stats, metric_name)
        plt.savefig(os.path.join(path, 'heatmap_{:s}.png'.format(metric_name)))
        plt.close(fig)
        plt.clf()

    # Plot a chart over time showing min/median/max worker stats
    for metric_name in metric_labels:
        for worker_id, metrics in stats.metrics.items():
            data = {
                'ids': [],
                'time': [],
                'value': [],
            }
            points = metrics[metric_name]
            current_time = quantization
            offset = 1
            summed_value = 0
            while offset < len(points):
                timePrev, valuePrev = points[offset - 1]
                time, value = points[offset]

                interval = float(time - timePrev)
                contribution = value / interval
                #print(metric_name, value, time, timePrev, interval)
                if time >= current_time:
                    # Add % contribution from prior interval
                    alpha = 1.0 - (time - current_time) / interval
                    #print(time, current_time, alpha)
                    summed_value += contribution * alpha

                    data['ids'].append(worker_id)
                    data['time'].append(current_time / 1e9)
                    data['value'].append(summed_value * 1e9) # to seconds
                    current_time += quantization
                    summed_value = 0

                    beta = 1.0 - alpha
                    summed_value += contribution * beta
                else:
                    summed_value += contribution
                offset += 1
            for i in range(len(data['time']) - 1):
                if data['time'][i + 1] < data['time'][i]:
                    print(data['time'][i], data['time'][i + 1])
            if False and metric_name == 'bytesReceived':
                print(data['time'], data['value'])
                print(points)
            plt.semilogy(data['time'], data['value'])
        plt.savefig(os.path.join(path, 'metric_{:s}.png'.format(metric_name)))
        plt.clf()


def calculate_run_time(run_stats):
    '''Calculate the time of a run under the stationary treelets model

    The time of a run is:


    '''

    boot_time = run_stats['boot_time']

    worker_max_bandwidth = run_stats['worker_max_bandwidth']
    total_bandwidth_used = (run_stats['total_bandwidth_used']
                            if 'total_bandwidth_used' in run_stats
                            else 0)

    total_rays = run_stats['total_rays']
    traversals_per_ray = run_stats['traversals_per_ray']
    total_ray_traversals = (run_stats['total_ray_traversals']
                            if 'total_ray_traversals' in run_stats
                            else total_rays * traversals_per_ray)

    ray_footprint = run_stats['ray_footprint']

    cpu_time_per_ray_traversal = run_stats['cpu_time_per_ray_traversal']

    num_workers = run_stats['num_workers']

    geometry_size = run_stats['geometry_size']
    num_treelets = run_stats['num_treelets']
    treelet_size = (run_stats['treelet_size']
                    if 'treelet_size' in run_stats
                    else geometry_size / num_treelets)
    
    # boot time + time to load treelets + max(io time, compute time)
    io_time = total_rays * traverasls_per_ray * ray_footprint / (num_workers * worker_max_bandwidth)
    compute_time = total_rays * cpu_time_per_ray_traversal / num_workers
    total_time = boot_time + treelet_size / worker_max_bandwidth + max(io_time, compute_time)
    return total_time


def calculate_model_run_time(scene_stats, num_workers):
    total_rays = scene_stats.total_rays * (2 * scene_stats.path_length - 1)
    traversals_per_ray = scene_stats.traversals_per_ray
    ray_footprint = scene_stats.ray_footprint
    
    total_bandwidth = total_rays * treelet_visits_per_ray * ray_footprint
    
    worker_bandwidth = Constants.WORKER_BANDWIDTH

    G = scene_stats.geometry_size
    TS = 0.1 # treelet size   
    TREELETS_PER_RAY = np.log(G / TS) / np.log(2)
    print(Constants.L)
    NR = 2200000000
    #L_B = 30/1000 
    L_B = 1/8
    BB_B = 5/8
    R = 64/(1000*1000*1000)
    
    stats = {
        'boot_time': Constants.LAMBDA_BOOT_TIME,
        'worker_max_bandwidth': Constants.LAMBDA_BANDWIDTH,
        'total_rays': total_rays,
        'traversasls_per_ray': traversals_per_ray,
        'ray_footprint': Constants.RAY_FOOTPRINT,
        'cpu_time_per_ray_traversal': Constants.CPU_TIME_PER_RAY_TRAVERSAL,
        'treelet_size': scene_stats.treelet_size,
    }
    total_times = [calculate_run_time({**stats, 'num_workers': w})
                  for w in num_workers]
    return total_times


def compare_model():
    # Get scene information
    scene_stats = SceneStats()
    num_workers = range(1, 8001)
    total_times = calculate_model_run_times(scene_stats, num_workers)
    costs = [t * w * Constants.LAMBDA_COST for t, w in zip(total_times, num_workers)]
    #plt.plot(price_kf, total_time_kf, label='kf')
    plt.plot(costs, total_times, label='lambda')
    #plt.hlines([L_BOOT_TIME + TS / L_B], 0, 15)
    plt.ylim(0, 500)
    plt.title('Render / $')
    plt.ylabel('Render time (seconds)')
    plt.xlabel('Dollars ($)')
     
    plt.legend()
    plt.show()
    plt.savefig('model.png')
    plt.clf()


def main():
    parser = argparse.ArgumentParser(description=(
        'Generate a trace file for viewing in chrome://tracing from cloud pbrt worker intervals.'))
    parser.add_argument('--worker-stats-path', default='worker_stats.txt',
                        help=(
                            'Path to the worker_intervals.txt generated by '
                            'pbrt-lambda-master after finished a run.'))
    parser.add_argument('--trace-path', default='pbrt.tar.gz',
                        help='Path to write the compressed trace file to.')
    parser.add_argument('--graph-path', default='graphs/',
                        help='Directory to write the generated graphs to.')
    args = parser.parse_args()

    #compare_model()
    print('Reading worker stats from {:s}...'.format(args.worker_stats_path))
    stats = WorkerStats(args.worker_stats_path)
    print('Done reading worker stats.')
    if False:
        path = write_trace(stats, args.trace_path)
        print('Wrote trace to {:s}.'.format(path))
    if True:
        path = args.graph_path
        plot_metrics(stats, path)
        print('Wrote graphs to {:s}.'.format(path))


if __name__ == "__main__":
    main()
