import sys
import struct
import json
import random
import tarfile
import os
import argparse
import pint
import math
import csv

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
from typing import List
from multiprocessing import Pool
from functools import partial
sns.set_style("ticks", {'axes.grid': True})
TREELET_TYPE = 0

def read_intervals(f, num_workers):
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

class WorkerDiagnostics(object):
    def __init__(self, idx, file_path : str):
        self.idx = idx
        self.path = file_path
        self.metrics = defaultdict(list)
        time_per_action = defaultdict(dict)
        bytes_sent_per_worker = defaultdict(dict)
        self.timestamps = []
        self.top_level_actions = set()
        with open(file_path, 'r') as f:
            _, start_timestamp = f.readline().strip().split(' ')
            self.start_timestamp = int(start_timestamp)

            for line in f:
                timestamp, json_data = line.strip().split(' ')
                timestamp = int(timestamp)
                self.timestamps.append(timestamp)
                data = json.loads(json_data)
                if 'timePerAction' in data:
                    for tup in data['timePerAction']:
                        name = tup['name']
                        time = int(tup['time'])
                        if ':' not in name:
                            self.top_level_actions.add(name)
                        time_per_action[name][timestamp] = time
                    del data['timePerAction']
                if 'metricsOverTime' in data:
                    del data['metricsOverTime']
                if 'intervalsPerAction' in data:
                    del data['intervalsPerAction']
                if 'bytesSentPerWorker' in data:
                    for k, v in data['bytesSentPerWorker'].items():
                        bytes_sent_per_worker[k][timestamp] = v
                    del data['bytesSentPerWorker']
                if 'bytesReceivedPerWorker' in data:
                    del data['bytesReceivedPerWorker']
                for name, value in data.items():
                    self.metrics[name].append(float(value))
        self.time_per_action = defaultdict(list)
        self.bytes_sent_per_worker = defaultdict(list)
        for t in self.timestamps:
            for k, v in time_per_action.items():
                if t in v:
                    value = v[t]
                else:
                    value = 0
                self.time_per_action[k].append(value)
            for k, v in bytes_sent_per_worker.items():
                if t in v:
                    value = v[t]
                else:
                    value = 0
                self.bytes_sent_per_worker[k].append(value)

    def percentage_action(self, action, idx=-1):
        if len(self.timestamps) == 0:
            return 0
        if idx < 0:
            total = sum(self.time_per_action[action])
            return total / self.timestamps[len(self.timestamps) - 1]
        else:
            assert len(self.timestamps) > idx
            start = self.timestamps[idx - 1] if idx > 0 else 0
            end = self.timestamps[idx]
            interval = end - start
            return self.time_per_action[action][idx] / interval

    def percentage_busy(self, idx=-1):
        return 1.0 - self.percentage_action('poll', idx)


class WorkerStats(object):
    def __init__(self, idx, file_path : str):
        self.idx = idx
        self.path = file_path

        self.aggregate_stats = defaultdict(list)
        self.queue_stats = defaultdict(list)
        treelet_stats = defaultdict(lambda: defaultdict(dict))
        stat_keys = set()
        self.timestamps = []
        with open(file_path, 'r') as f:
            _, start_timestamp = f.readline().strip().split(' ')
            self.start_timestamp = int(start_timestamp)

            prev_data = defaultdict(dict)
            for line in f:
                split_line  = line.strip().split(' ')
                if len(split_line) < 2:
                    continue
                timestamp, json_data = split_line
                timestamp = int(timestamp)
                self.timestamps.append(timestamp)
                data = json.loads(json_data)
                for k, v in data['aggregateStats'].items():
                    self.aggregate_stats[k].append(v)
                for k, v in data['queueStats'].items():
                    self.queue_stats[k].append(v)
                for ray_stats in data['objectStats']:
                    object_key = ray_stats['id']
                    typ = object_key['type']
                    typ_id = int(object_key['id'])
                    stats = ray_stats['stats']
                    if typ != TREELET_TYPE:
                        continue
                    for metric_name, v in stats.items():
                        prev_v = 0
                        if (typ_id in prev_data and
                            metric_name in prev_data[typ_id]):
                            prev_v = prev_data[typ_id][metric_name]
                        treelet_stats[typ_id][metric_name][timestamp] = float(v) - prev_v
                        prev_data[typ_id][metric_name] = float(v)
                        stat_keys.add(metric_name)

        self.treelet_stats = {}
        for treelet_id in treelet_stats.keys():
            self.treelet_stats[treelet_id] = {}
            for stat_key in stat_keys:
                self.treelet_stats[treelet_id][stat_key] = []
                ll = self.treelet_stats[treelet_id][stat_key]
                tvs = treelet_stats[treelet_id][stat_key]
                for t in self.timestamps:
                    if t in tvs:
                        v = tvs[t]
                    else:
                        v = 0
                    ll.append(v)

def _load_fn(diagnostics_directory, path):
    diag = None
    if path.endswith('DIAG'):
        diag = WorkerDiagnostics(int(path[:path.find('.DIAG')]),
                                 os.path.join(diagnostics_directory, path))
    stats = None
    if path.endswith('STATS'):
        stats = WorkerStats(int(path[:path.find('.STATS')]),
                            os.path.join(diagnostics_directory, path))
    print('.', end='', flush=True)
    return diag, stats

class Stats(object):
    def __init__(self, diagnostics_directory : str):
        worker_files = os.listdir(diagnostics_directory)
        self.worker_diagnostics = []
        self.worker_stats = []
        with Pool() as p:
            results = p.map(partial(_load_fn, diagnostics_directory), worker_files)
            for d, s in results:
                if d:
                    self.worker_diagnostics.append(d)
                if s:
                    self.worker_stats.append(s)

    def write_csv(self, path: str):
        quanta = 2.0 * 1e6 # seconds -> microseconds
        start_timestamp = self.worker_stats[0].start_timestamp
        end_timestamp = 0
        for stats in self.worker_stats:
            start_ts = stats.start_timestamp
            end_ts = start_ts + stats.timestamps[-1]
            if start_ts < start_timestamp or end_ts > end_timestamp:
                print('Current min {:d}, max {:d}'.format(start_timestamp, end_timestamp))
                print('Worker id {:d}, min {:d}, max {:d}, path {:s}'.format(stats.idx,
                                                                  start_ts, end_ts, stats.path))
            start_timestamp = min(start_timestamp, start_ts)
            end_timestamp = max(end_timestamp, end_ts)

        #for diag in self.worker_diagnostics:
        #    start_ts = diag.start_timestamp
        #    end_ts = start_ts + stats.timestamps[-1]
        #    if start_ts < start_timestamp or end_ts > end_timestamp:
        #        print('Current min {:d}, max {:d}'.format(start_timestamp, end_timestamp))
        #        print('Worker id {:d}, min {:d}, max {:d}, path {:s}'.format(diag.idx,
        #                                                                     start_ts, end_ts, diag.path))
        #    start_timestamp = min(start_timestamp, start_ts)
        #    end_timestamp = max(end_timestamp, end_ts)

        end_timestamp += quanta * 2

        total_duration = end_timestamp - start_timestamp
        num_timepoints = int(math.ceil(total_duration / quanta))
        print('Time bounds: ({:f}, {:f}), {:f} seconds'.format(start_timestamp * 1e-6,  end_timestamp * 1e-6,
                                                         total_duration * 1e-6))
        print('Quantizing to {:2f} second interval'.format(quanta * 1e-6))
        print('# timepoints:', num_timepoints)

        # Pre-process data to fixed quantization
        #fieldnames = ['workerID', 'treeletID', 'timestamp',
        #              'tracingRaysTime', 'idleTime',
        #              'raysWaiting', 'raysProcessed', 'raysGenerated', 'raysSending', 'raysReceived',
        #              'bandwidthIn', 'bandwidthOut']
        # bandwidth, tracing time
        per_worker_data = {}
        per_worker_worker_fieldnames = [
            'workerID', 'targetWorkerID', 'timestamp', 'bytesSent']
        per_worker_worker_data = {}
        # rays processed,  rays generated, etc
        per_worker_treelet_fieldnames = [
            'workerID', 'treeletID', 'timestamp',
            'raysWaiting', 'raysProcessed', 'raysGenerated', 'raysSending', 'raysReceived', 'raysResent', 'raysSent']
        per_worker_treelet_data = {}
        csv_data = {}
        print('Quantizing worker stats', end='', flush=True)
        for stats in self.worker_stats:
            min_timestamp = stats.start_timestamp
            max_timestamp = min_timestamp + stats.timestamps[-1]
            # Get the treelet fields and quantize to global time scale
            treelet_rows = defaultdict(lambda: defaultdict(list))
            for treelet_id, treelet_stats in stats.treelet_stats.items():
                for field, key in [('processedRays', 'raysProcessed'),
                                   ('waitingRays', 'raysWaiting'),
                                   ('demandedRays', 'raysGenerated'),
                                   ('sendingRays', 'raysSending'),
                                   ('receivedRays', 'raysReceived'),
                                   ('resentRays', 'raysResent'),
                                   ('sentRays', 'raysSent')]:
                    quantized_timestamps, quantized_data = quantize_sequence(
                        [min_timestamp] + [min_timestamp + x for x in stats.timestamps],
                        [0] + treelet_stats[field],
                        quanta,
                        start=start_timestamp, end=end_timestamp)
                    treelet_rows[treelet_id][key] = quantized_data

                    # NOTE(apoms): THIS IS DEBUGGING CODE
                    #
                    # quant_sum = sum(quantized_data)
                    # data_sum = sum(treelet_stats[field])
                    # error = np.abs(quant_sum - data_sum)
                    # if error > 0.1:
                    #     print('Error: {:f}, quant {:f}, data {:f}'.format(error, quant_sum, data_sum))
                    #     quant_nz = [(x, t) for x, t in zip(quantized_data, quantized_timestamps)
                    #                 if x != 0]
                    #     quant_cumsum = [quant_nz[0][0]]
                    #     for x in quant_nz[1:]:
                    #         quant_cumsum.append(x[0] + quant_cumsum[-1])
                    #     treelet_nz = [(x, t) for x, t in zip(treelet_stats[field],
                    #                                          [min_timestamp + x for x in stats.timestamps])
                    #                   if x != 0]
                    #     treelet_cumsum = [treelet_nz[0][0]]
                    #     for x in treelet_nz[1:]:
                    #         treelet_cumsum.append(x[0] + treelet_cumsum[-1])

                    #     from pprint import pprint
                    #     pprint(s)
                    #     print('Data : Quant')
                    #     quant_offset = 0
                    #     data_offset = 0
                    #     for i in range(len(treelet_nz) + len(quant_nz)):
                    #         if len(treelet_nz) > data_offset and len(quant_nz) > quant_offset:
                    #             dt = treelet_nz[data_offset][1] / 1e9
                    #             dq = quant_nz[quant_offset][1] / 1e9
                    #             if dt > dq:
                    #                 d = quant_nz[quant_offset][1] / 1e13
                    #                 x = quant_cumsum[quant_offset]
                    #                 print('\t\t', end='')
                    #                 print('{:f}: {:f}'.format(d, x), end='')
                    #                 quant_offset += 1
                    #             else:
                    #                 d = treelet_nz[data_offset][1] / 1e13
                    #                 x = treelet_cumsum[data_offset]
                    #                 print('{:f}: {:f}'.format(d, x), end='')
                    #                 data_offset += 1
                    #         elif len(treelet_nz) > data_offset:
                    #             d = treelet_nz[data_offset][1] / 1e13
                    #             x = treelet_cumsum[data_offset]
                    #             print('{:f}: {:f}'.format(d, x), end='')
                    #             data_offset += 1
                    #         else:
                    #             d = quant_nz[quant_offset][1] / 1e13
                    #             x = quant_cumsum[quant_offset]
                    #             print('\tt', end='')
                    #             print('{:f}: {:f}'.format(d, x), end='')
                    #             quant_offset += 1
                    #         print()
            # Insert rows into per_worker_treelet_data
            for treelet_id, _ in stats.treelet_stats.items():
                for i in range(num_timepoints):
                    timepoint = start_timestamp + quanta * i
                    data = {
                        'workerID': stats.idx,
                        'treeletID': treelet_id,
                        'timestamp': timepoint,
                    }
                    for k, v in treelet_rows[treelet_id].items():
                        data[k] = v[i]
                    per_worker_treelet_data[(stats.idx, treelet_id, timepoint)] = data
            print('.', end='', flush=True)
        print()

        for diag in self.worker_diagnostics:
            for treelet_id, treelet_stats in stats.treelet_stats.items():
                for tidx in range(num_timepoints):
                    # tracingRaysTime
                    timestamp = tidx * quanta


        print('Writing {:s}...'.format(path))
        with open(path, 'w', newline='') as csvfile:
            writer  = csv.DictWriter(csvfile, fieldnames=per_worker_treelet_fieldnames)
            writer.writeheader()
            for k, v in per_worker_treelet_data.items():
                writer.writerow(v)
        print('Wrote {:s}.'.format(path))


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


def merge_sequences(timepoints : List[List[int]],
                    data : List[List[float]]):
    raise Exception()

    merged_timepoints = []
    merged_data = []

    num_sequences = len(timepoints)
    offsets = [0 for _ in range(num_sequences)]
    while True:
        for i in range(num_sequences):
            pass


    return merged_timepoints, merged_data


def quantize_sequence(timepoints, data, quanta, start=None, end=None, rate=False):
    quantized_timepoints = []
    quantized_data = []

    def push(timepoint, data):
        quantized_timepoints.append(timepoint)
        quantized_data.append(data)

    if len(data) == 0:
        return []

    current_time = start or timepoints[0]
    end_time = end or timepoints[-1]

    # Insert zeros up to the start of timepoints if the start time is before
    # timepoints
    while current_time + quanta < timepoints[0]:
        push(current_time, 0)
        current_time += quanta

    # Find the starting offset if the start is inside the sequence
    offset = 1
    if start:
        while offset < len(timepoints):
            time = timepoints[offset]
            if time > current_time:
                break
            offset += 1

    current_time_in_interval = max(timepoints[offset - 1], current_time)
    prev_summed_value = 0
    summed_value = 0
    # Sum over multiple timepoints
    while offset < len(timepoints) and current_time < end_time:
        # Invariant: timepoints[offset - 1] <= current_time + quanta < timepoints[offset]
        prev_timepoint = timepoints[offset - 1]
        prev_value = data[offset - 1]
        timepoint = timepoints[offset]
        value = data[offset]

        interval = float(timepoint - prev_timepoint)
        contribution = value
        if rate:
            contribution /= interval

        # Add the contribution from this interval
        next_time_in_interval = min(timepoint, current_time + quanta)
        this_interval = float(next_time_in_interval - current_time_in_interval)
        if this_interval > 0:
            this_contribution = value * (this_interval / interval)
            if rate:
                 this_contribution /= this_interval
        else:
            this_contribution = 0

        summed_value += this_contribution

        if current_time + quanta <= timepoint:
            # We've reached the end of this quanta, so append a new entry
            push(current_time, prev_summed_value)
            prev_summed_value = summed_value
            summed_value = 0
            current_time += quanta
            current_time_in_interval = current_time
            if current_time > timepoint:
                # Step to the next timepoint only if we've moved past the current
                # one
                offset += 1
        else:
            # Still inside the quanta, so step to the next timepoint
            current_time_in_interval = timepoint
            offset += 1

    # Add zeros at the end of the sequence
    while current_time < end_time:
        push(current_time, prev_summed_value)
        prev_summed_value = summed_value
        summed_value = 0
        current_time += quanta

    return quantized_timepoints, quantized_data


def heatmap(data, row_labels, col_labels, ax=None,
            cbar_kw={}, cbarlabel="",
            dense_xticks=False,
            dense_yticks=False,
            **kwargs):
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
    im = ax.imshow(data, **kwargs)

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")

    # We want to show all ticks...
    #ax.set_xticks(np.arange(data.shape[1]))
    # ... and label them with the respective list entries.
    if dense_xticks:
        xtick_spacing = 1
    else:
        xtick_spacing = (data.shape[1] // 5) or 1
    ax.set_xticks(np.arange(data.shape[1])[::xtick_spacing])
    ax.set_xticklabels(col_labels[::xtick_spacing])

    if dense_yticks:
        ytick_spacing = 1
    else:
        ytick_spacing = (data.shape[0] //  5) or 1
    ax.set_yticks(np.arange(data.shape[0])[::ytick_spacing])
    ax.set_yticklabels(row_labels[::ytick_spacing])

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
    plt.tight_layout()

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


def plot_metric_heatmap(stats, metric_label, sort=False):
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


def plot_action_heatmap(worker_stats):
    num_worker_timestamps = len(worker_stats.timestamps)

    # Deteremine number of rows
    num_timepoints = num_worker_timestamps

    # Deteremine number of columns
    num_actions = len(worker_stats.top_level_actions)

    data = np.zeros((num_actions, num_timepoints))
    row_labels = []
    for action_idx, action_name in enumerate(sorted(list(worker_stats.top_level_actions))):
        row_labels.append(action_name)
        points = worker_stats.time_per_action[action_name]
        for i in range(num_timepoints):
            p = worker_stats.percentage_action(action_name, i)
            data[action_idx, i] = p
    col_labels = [int(x * 1e-6) for x in worker_stats.timestamps]
    fig = plt.figure(dpi=300)
    ax = plt.subplot()
    im, cbar = heatmap(data, row_labels, col_labels, ax=ax, dense_yticks=True, aspect='auto')
    return fig


def plot_worker_heatmap(all_worker_stats, metric):
    '''Plot a treelet metric over time for all treelets'''
    workers = {}
    for w in all_worker_stats:
        workers[int(w.idx)] = w
    # Number of rows
    worker_ids = list(sorted(workers.keys()))

    # Determine number of columns
    timestamp_sets = [set(stats.timestamps) for stats in all_worker_stats]
    timestamps = set()
    for s in timestamp_sets:
        timestamps = timestamps.union(s)
    timestamps = sorted(list(timestamps))
    num_timepoints = len(timestamps)

    data = np.zeros((len(worker_ids), num_timepoints))
    row_labels = []
    for i, idx in enumerate(worker_ids):
        row_labels.append(idx)
        points = workers[idx].aggregate_stats[metric]
        ts = workers[idx].timestamps
        for j, t in enumerate(timestamps):
            if t in ts:
                data[i, j] = points[ts.index(t)]
    col_labels = [int(x * 1e-6) for x in timestamps]
    fig = plt.figure(dpi=300)
    ax = plt.subplot()
    im, cbar = heatmap(data, row_labels, col_labels, ax=ax, aspect='auto')
    return fig


def plot_utilization_heatmap(all_worker_diags):
    '''Plot a treelet metric over time for all treelets'''
    workers = {}
    for w in all_worker_diags:
        workers[int(w.idx)] = w
    # Number of rows
    worker_ids = list(sorted(workers.keys()))

    # Determine number of columns
    timestamp_sets = [set(stats.timestamps) for stats in all_worker_diags]
    timestamps = set()
    for s in timestamp_sets:
        timestamps = timestamps.union(s)
    timestamps = sorted(list(timestamps))
    num_timepoints = len(timestamps)

    data = np.zeros((len(worker_ids), num_timepoints))
    row_labels = []
    for i, idx in enumerate(worker_ids):
        row_labels.append(idx)
        ts = workers[idx].timestamps
        for j, t in enumerate(timestamps):
            if t in ts:
                data[i, j] = workers[idx].percentage_busy(ts.index(t))
    col_labels = [int(x * 1e-6) for x in timestamps]
    fig = plt.figure(dpi=300)
    ax = plt.subplot()
    im, cbar = heatmap(data, row_labels, col_labels, ax=ax, aspect='auto')
    return fig


def aggregate_treelet_stats(all_worker_stats):
    # Get the list of treelet ids and stat keys
    treelet_ids = set()
    stat_keys = set()
    for worker in all_worker_stats:
        for treelet_id, treelet_stats in worker.treelet_stats.items():
            treelet_ids.add(treelet_id)
            for k, _ in treelet_stats.items():
                stat_keys.add(k)

    # Determine number of columns
    timestamp_sets = [set(stats.timestamps) for stats in all_worker_stats]
    timestamps = set()
    for s in timestamp_sets:
        timestamps = timestamps.union(s)
    timestamps = sorted(list(timestamps))
    num_timepoints = len(timestamps)

    # Aggregate treelet stats across all workers
    treelet_stats = {}
    for tid in treelet_ids:
        treelet_stats[tid] = {}
        for key in stat_keys:
            treelet_stats[tid][key] = [0 for _ in range(num_timepoints)]

    for worker_stats in all_worker_stats:
        for i, t in enumerate(timestamps):
            if t in worker_stats.timestamps:
                idx = worker_stats.timestamps.index(t)
                for treelet_id, wtreelet_stats in worker_stats.treelet_stats.items():
                    for key in stat_keys:
                        treelet_stats[treelet_id][key][i] += int(wtreelet_stats[key][idx])

    return treelet_stats, timestamps


def plot_treelet_heatmap(treelet_stats, timestamps, metric):
    '''Plot a treelet metric over time for all treelets'''
    # Number of rows
    treelet_ids = sorted([int(x) for x in list(treelet_stats.keys())])
    # Number of columns
    num_timepoints = len(timestamps)

    data = np.zeros((len(treelet_ids), num_timepoints))
    row_labels = []
    for idx, treelet_id in enumerate(treelet_ids):
        row_labels.append(treelet_id)
        points = treelet_stats[treelet_id][metric]
        print(idx, end=' ')
        for i, d in enumerate(points):
            data[idx, i] = d
            if d > 100000:
                print(d, end=' ')
        print()
    col_labels = [int(x * 1e-6) for x in timestamps]
    fig = plt.figure(dpi=300)
    ax = plt.subplot()
    im, cbar = heatmap(data, row_labels, col_labels, ax=ax, aspect='auto')
    return fig


def plot_treelet_worker_rays(treelet_stats, worker_stats, metric, max_workers=15, normalized=False):
    '''Plot distribution of treelet rays to workers'''
    # Number of rows
    treelet_ids = sorted([int(x) for x in list(treelet_stats.keys())])
    # Number of columns
    num_workers = min(len(worker_stats), max_workers)

    data = np.zeros((len(treelet_ids), num_workers))
    row_labels = []
    for idx, treelet_id in enumerate(treelet_ids):
        row_labels.append(treelet_id)
        treelet_value = sum(treelet_stats[treelet_id][metric]) or 0.00001
        values = []
        for i in range(len(worker_stats)):
            stats = worker_stats[i]
            value = 0
            if treelet_id in stats.treelet_stats:
                value = sum([int(x) for x in stats.treelet_stats[treelet_id][metric]])
            if normalized:
                value = value / treelet_value
            values.append(value)
        values.sort()
        values = list(reversed(values))
        for i in range(num_workers):
            data[idx, i] = values[i]

    col_labels = range(num_workers)
    fig = plt.figure(dpi=300)
    ax = plt.subplot()
    im, cbar = heatmap(data, row_labels, col_labels, ax=ax, aspect='auto')
    return fig


def plot_treelet_worker_rays(treelet_stats, worker_stats, metric, max_workers=15, normalized=False):
    '''Plot distribution of treelet rays to workers'''
    # Number of rows
    treelet_ids = sorted([int(x) for x in list(treelet_stats.keys())])
    # Number of columns
    num_workers = min(len(worker_stats), max_workers)

    data = np.zeros((len(treelet_ids), num_workers))
    row_labels = []
    for idx, treelet_id in enumerate(treelet_ids):
        row_labels.append(treelet_id)
        treelet_value = sum(treelet_stats[treelet_id][metric]) or 0.00001
        values = []
        for i in range(len(worker_stats)):
            stats = worker_stats[i]
            value = 0
            if treelet_id in stats.treelet_stats:
                value = sum([int(x) for x in stats.treelet_stats[treelet_id][metric]])
            if normalized:
                value = value / treelet_value
            values.append(value)
        values.sort()
        values = list(reversed(values))
        for i in range(num_workers):
            data[idx, i] = values[i]

    col_labels = range(num_workers)
    fig = plt.figure(dpi=300)
    ax = plt.subplot()
    im, cbar = heatmap(data, row_labels, col_labels, ax=ax, aspect='auto')
    return fig


def plot_metrics(stats, path):
    quantization = 1000 * 1e6 # nanoseconds

    plt.clf()
    metric_labels = ['bytesSent', 'bytesReceived', 'outstandingUdp']
    # Plot heatmaps for each worker over time
    # for metric_name in metric_labels:
    #     fig = plot_metric_heatmap(stats, metric_name)
    #     plt.savefig(os.path.join(path, 'heatmap_{:s}.png'.format(metric_name)))
    #     plt.close(fig)
    #     plt.clf()

    # Plot heatmaps showing time spent in each action for least/most idle worker, and average worker
    most_busy = 0.0
    most_busy_worker = stats.worker_diagnostics[0]
    least_busy = 1.0
    least_busy_worker = stats.worker_diagnostics[0]
    for worker in stats.worker_diagnostics:
        if len(worker.timestamps) == 0:
            continue
        busy = worker.percentage_busy() * len(worker.timestamps)
        if busy > most_busy:
            most_busy = busy
            most_busy_worker = worker
        if busy < least_busy:
            least_busy = busy
            least_busy_worker = worker

    if len(most_busy_worker.timestamps) > 0:
        fig = plot_action_heatmap(most_busy_worker)
        plt.savefig(os.path.join(path, 'most_busy_worker_action_heatmap.png'))
        plt.close(fig)
        plt.clf()
        print('Graphed most busy worker action heatmap.')
    if len(least_busy_worker.timestamps) > 0:
        fig = plot_action_heatmap(least_busy_worker)
        plt.savefig(os.path.join(path, 'least_busy_worker_action_heatmap.png'))
        plt.close(fig)
        plt.clf()
        print('Graphed least busy worker action heatmap.')

    print('Graphing worker heatmaps...')
    plot_utilization_heatmap(stats.worker_diagnostics)
    plt.savefig(os.path.join(path, 'worker_utilization_heatmap.png'))
    plt.close(fig)
    plt.clf()
    for metric in ['processedRays', 'receivedRays', 'sendingRays']:
        plot_worker_heatmap(stats.worker_stats, metric)
        plt.savefig(os.path.join(path, 'worker_heatmap_{:s}.png'.format(metric)))
        plt.close(fig)
        plt.clf()
    print('Graphed worker heatmaps.')

    print('Aggregating treelet stats...')
    treelet_stats, timestamps = aggregate_treelet_stats(stats.worker_stats)
    print('Done aggregating treelet stats.')
    for metric in ['sendingRays', 'processedRays']:
        plot_treelet_heatmap(treelet_stats, timestamps, metric)
        plt.savefig(os.path.join(path, 'treelet_heatmap_{:s}.png'.format(metric)))
        plt.close(fig)
        plt.clf()
    print('Graphed treelet heatmap.')

    plot_treelet_worker_rays(treelet_stats, stats.worker_stats, 'processedRays', normalized=True)
    plt.savefig(os.path.join(path, 'treelet_worker_processedRays_normalized.png'))
    plt.close(fig)
    plt.clf()
    plot_treelet_worker_rays(treelet_stats, stats.worker_stats, 'processedRays')
    plt.savefig(os.path.join(path, 'treelet_worker_processedRays.png'))
    plt.close(fig)
    plt.clf()
    print('Graphed treelet-worker heatmap.')

    return

    # Plot a chart over time showing min/median/max worker stats
    for metric_name in metric_labels:
        for worker_id, metrics in worker_stats.metrics.items():
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
    parser.add_argument('--diagnostics-directory', default='diag',
                        help=(
                            'Path to the master_stats.txt generated by '
                            'pbrt-lambda-master after finished a run.'))
    parser.add_argument('--trace-path', default='pbrt.tar.gz',
                        help='Path to write the compressed trace file to.')
    parser.add_argument('--graph-path', default='graphs/',
                        help='Directory to write the generated graphs to.')
    args = parser.parse_args()
    #compare_model()
    print('Reading diagnostics from {:s}...'.format(args.diagnostics_directory), end='')
    diagnostics = Stats(args.diagnostics_directory)
    print()
    diagnostics.write_csv('test.csv')
    print('Done reading diagnostics.')
    if False:
        path = write_trace(stats, args.trace_path)
        print('Wrote trace to {:s}.'.format(path))
    if True:
        path = args.graph_path
        plot_metrics(diagnostics, path)
        print('Wrote graphs to {:s}.'.format(path))


if __name__ == "__main__":
    main()
