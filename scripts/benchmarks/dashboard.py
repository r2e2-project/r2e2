#!/usr/bin/env python3

import argparse
import json
import matplotlib.pyplot as plt
from decimal import Decimal

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--cloud', type=argparse.FileType('r'),
                    required=True)
parser.add_argument('-p', '--pbrt', type=argparse.FileType('r'),
                    required=True)
parser.add_argument('absolute', type=str)
parser.add_argument('relative', type=str)

args = parser.parse_args()

cloud_stats = json.load(args.cloud)
pbrt_stats = json.load(args.pbrt)

fig, ax = plt.subplots()

scenes = list(cloud_stats.keys() & pbrt_stats.keys())
scenes.sort()

x = range(len(scenes))
width = 0.2
ax.set_xticks([i + width/2 for i in x])
ax.set_xticklabels(scenes)

cloud = [cloud_stats[scene] for scene in scenes]
pbrt = [pbrt_stats[scene] for scene in scenes]

def bar_with_label(x, y, *args, **kwargs):
    ax.bar(x, y, *args, **kwargs)
    for a, b in zip(x, y):
        label = '%.2e' % Decimal(b) if b < 1 else str(b)
        ax.text(a, b*1.01, label,
                color='black', fontweight='normal', ha='center')


bar_with_label(x, cloud, width, color='#0F52BA', label='CloudRT')
bar_with_label([i + width for i in x], pbrt, width, color='#73C2FB', label='PBRT')

ax.set_yscale('log')
ax.set_ylabel('Rays / core / second')
ax.legend(bbox_to_anchor=(1.0, 1.15))

plt.title('Performance')
plt.savefig(args.absolute, dpi=300)

plt.clf()
fig, ax = plt.subplots()

ax.set_ylabel('CloudRT Perf / PBRT Perf')
ax.set_xticks([i for i in x])
ax.set_xticklabels(scenes)

bar_with_label(x, [i[0] / i[1] for i in zip(cloud, pbrt)], color='#73C2FB')

plt.title('Performance')
plt.savefig(args.relative, dpi=300)
