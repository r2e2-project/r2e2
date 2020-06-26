#!/usr/bin/env python3

import os
import sys
import datetime
import argparse
import json
import tempfile

from collections import OrderedDict

import boto3
import botocore
import pandas as pd
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--bucket', required=True)
parser.add_argument('-s', '--scene-name', required=True)
parser.add_argument('-r', '--run-name', required=True)
parser.add_argument('-d', '--directory', required=True)

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'p%s' % n
    return percentile_


def generate_info_json(info, alloc_df, out):
    alloc_data = alloc_df.groupby(['treeletId']).agg({'workerId': 'count'})
    info['allocation'] = alloc_data.workerId.tolist()

    with open(out, "w") as fout:
        json.dump(info, fout)


def generate_data_csv(df, out):
    df['timestampS'] = (df.timestamp / 1000).astype('int32')
    df['totalTransferred'] = df.bytesEnqueued + df.bytesDequeued
    df['bytesPerBag'] = (df.bytesEnqueued / df.bagsEnqueued)

    data = df.groupby(['timestampS']).agg({
        'workerId': ['nunique'],
        'raysEnqueued': ['sum'],
        'raysDequeued': ['sum'],
        'bytesEnqueued': ['sum'],
        'bytesDequeued': ['sum'],
        'totalTransferred': ['sum'],
        'bytesPerBag': ['mean'],
        'pathsFinished': ['sum'],
        'cpuUsage': ['mean', 'median', percentile(90), percentile(95)]
    }).sort_values('timestampS')

    data['runningCompletion'] = data.pathsFinished.cumsum()
    finish_point = data.runningCompletion.idxmax()
    data = data.loc[data.index <= finish_point]
    data.columns = [x[0] if x[1] in ["", "sum"]
                    else "_".join(x) for x in data.columns.ravel()]

    data.fillna(method='ffill', inplace=True)

    data.to_csv(out)


def generate_treelet_csv(df, out):
    df['timestampS'] = (df.timestamp / 1000).astype('int32')

    data = (df.sort_values(['timestampS', 'treeletId'])
              .groupby(['timestampS', 'treeletId']).sum())

    data = data.reset_index()
    data = data[['timestampS', 'treeletId', 'raysEnqueued', 'raysDequeued']]

    data.to_csv(out, index=False)


def generate_worker_csv(df, alloc_df, out):
    allocation_data = (alloc_df.sort_values(['treeletId'])
                       .reset_index(drop=True)
                       .reset_index()
                       .rename(columns={'index': 'tempWorkerId'}))

    data = df.merge(allocation_data, on='workerId', how='left')
    del data['workerId']

    data['timestampS'] = (data.timestamp / 1000).astype('int32')

    data = (data.sort_values(['timestampS', 'treeletId', 'tempWorkerId'])
                .groupby(['timestampS', 'tempWorkerId']).agg(
                    {
                        'treeletId': 'max',
                        'raysDequeued': 'sum',
                        'raysEnqueued': 'sum'
                    }))

    data = data.reset_index()
    data = data[['timestampS', 'tempWorkerId',
                 'treeletId', 'raysEnqueued', 'raysDequeued']]

    data = data[(data.raysDequeued > 0) | (data.raysEnqueued > 0)]

    data.to_csv(out, index=False)


def dashboard_obj(x):
    return os.path.join("dashboard", x)


def main(bucket, scene_name, run_name, logs_directory):
    SCENE_LIST = dashboard_obj("scenes.json")

    scenes = []

    # Step (1): get scene list
    try:
        response = s3_client.get_object(Bucket=bucket,
                                        Key=SCENE_LIST)
        scenes = json.loads(response['Body'].read().decode('utf-8'))['scenes']
    except botocore.exceptions.ClientError:
        pass
    except KeyError:
        pass

    if scene_name not in scenes:
        scenes += [scene_name]
        s3_client.put_object(ACL='public-read',
                             Body=json.dumps(
                                 {"scenes": scenes}).encode('utf-8'),
                             Bucket=bucket,
                             Key=SCENE_LIST)

    # Step (2): get runs list
    runs = []
    RUN_LIST = os.path.join("dashboard", scene_name, "runs.json")

    try:
        response = s3_client.get_object(Bucket=bucket,
                                        Key=RUN_LIST)
        runs = json.loads(response['Body'].read().decode('utf-8'))['runs']
    except botocore.exceptions.ClientError:
        pass
    except KeyError:
        pass

    matching_runs_idx = [i for i in range(
        len(runs)) if runs[i]['name'] == run_name]

    if matching_runs_idx:
        q = input("A run with this name already exists. Replace [y/N]? ")
        if q == 'y':
            for idx in matching_runs_idx[::-1]:
                del runs[idx]
        else:
            return

    runs += [{
        "name": run_name,
        "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }]

    s3_client.put_object(ACL='public-read',
                         Body=json.dumps(
                             {"runs": runs}).encode('utf-8'),
                         Bucket=bucket,
                         Key=RUN_LIST)

    RUN_DIR = os.path.join("dashboard", scene_name, run_name)

    # Generate necessary files
    with tempfile.NamedTemporaryFile() as df, \
            tempfile.NamedTemporaryFile() as tf, \
            tempfile.NamedTemporaryFile() as wf, \
            tempfile.NamedTemporaryFile() as nf:

        with open(os.path.join(logs_directory, 'info.json')) as fin:
            info = json.load(fin, object_pairs_hook=OrderedDict)

        allocation_data = pd.read_csv(
            os.path.join(logs_directory, 'allocations.csv'))
        worker_data = pd.read_csv(os.path.join(logs_directory, 'workers.csv'))
        treelet_data = pd.read_csv(
            os.path.join(logs_directory, 'treelets.csv'))

        del allocation_data['action']

        generate_info_json(info, allocation_data, nf.name)
        generate_worker_csv(worker_data, allocation_data, wf.name)
        generate_data_csv(worker_data, df.name)
        generate_treelet_csv(treelet_data, tf.name)

        df.flush()
        tf.flush()
        wf.flush()
        nf.flush()

        print("Uploading 'info.json'... ", end='')
        s3_client.upload_file(Filename=nf.name,
                              Bucket=bucket,
                              Key=os.path.join(RUN_DIR, "info.json"),
                              ExtraArgs={'ACL': 'public-read'})
        print("done.")

        print("Uploading 'data.csv'... ", end='')
        s3_client.upload_file(Filename=df.name,
                              Bucket=bucket,
                              Key=os.path.join(RUN_DIR, "data.csv"),
                              ExtraArgs={'ACL': 'public-read'})
        print("done.")

        print("Uploading 'treelet.csv'... ", end='')
        s3_client.upload_file(Filename=tf.name,
                              Bucket=bucket,
                              Key=os.path.join(RUN_DIR, "treelet.csv"),
                              ExtraArgs={'ACL': 'public-read'})
        print("done.")

        print("Uploading 'worker.csv'... ", end='')
        s3_client.upload_file(Filename=wf.name,
                              Bucket=bucket,
                              Key=os.path.join(RUN_DIR, "worker.csv"),
                              ExtraArgs={'ACL': 'public-read'})
        print("done.")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args.bucket, args.scene_name, args.run_name, args.directory)
