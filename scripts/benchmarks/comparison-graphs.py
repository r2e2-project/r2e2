#!/usr/bin/env python3

import pandas as pd
import argparse
import os
import matplotlib.pyplot as plt
import theoretical_model
import json

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--data', type=str, required=True)
parser.add_argument('-o', '--out', type=str, required=True)
parser.add_argument('-s', '--scene-size', type=float, required=True)
parser.add_argument('-i', '--ignore-startup', action='store_true')
parser.add_argument('title')

args = parser.parse_args()
title_prefix = ' '.join(args.title)

data = []
for name in os.listdir(args.data):
    dir = os.path.join(args.data, name)

    try:
        with open(os.path.join(dir, 'master.json'), 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        continue

    info['name'] = name
    spp = name.split('-')[-1]
    info['spp'] = int(spp)
    df = pd.DataFrame(info, index=[0])
    df = df[['name', 'numLambdas', 'spp', 'totalTime', 'launchTime', 'rayTime', 'totalPaths']]

    data.append(df)

data = pd.concat(data, axis=0).sort_values(['num_lambdas', 'spp']).reset_index(drop=True)

print(data)

def setup_graph(title):
    plt.clf()
    plt.title(title)
    plt.xlabel('Number of workers')
    plt.ylabel('Render time (seconds)')

for spp in data.spp.unique():
    subset = data[data.spp == spp]
    title = '{}: {}spp'.format(title_prefix, spp)
    setup_graph(title)

    fname = title.replace(' ', '-').replace(':', '') + '.png'
    plt.plot(subset.num_lambdas,
             subset.ray_time if args.ignore_startup else subset.total_time,
             label='Real')

    num_paths = subset.num_paths.min() # Assumes all data points have same image size
    pred_range = range(subset.num_lambdas.min(), subset.num_lambdas.max() + 1, 10)
    preds = [theoretical_model.prediction(scene_size=args.scene_size,
        num_rays=num_paths, path_length=2,
        num_workers=nlambdas,
        startup_time=not args.ignore_startup) for nlambdas in pred_range]

    plt.plot(pred_range, preds, label='Model')

    plt.legend()
    plt.savefig(os.path.join(args.out, fname), dpi=300)
