#!/usr/bin/env python3

import os
import sys
import numpy as np
import pandas as pd

AVERAGE_BANDWIDTH = 50_000_000  # 50 MB/s
SHADE_CUTOFF = 100


def calculate_compute_time(d, mean_shade_time):
    if d.trace > 0:
        return d.traceTime / 2
    elif d.shade > SHADE_CUTOFF:
        return d.shadeTime / 1.5
    elif d.shade > 0:
        return (mean_shade_time * d.shade) / 1.5
    else:
        return 0


def get_data(path):
    workers = pd.read_csv(os.path.join(path, "workers.csv"))
    summary = pd.read_csv(os.path.join(path, "summary.csv"))

    workers = (
        workers.sort_values("workerId")
        .groupby("workerId")
        .agg(
            {
                "workerId": "max",
                "raysEnqueued": "sum",
                "raysDequeued": "sum",
                "bytesEnqueued": "sum",
                "bytesDequeued": "sum",
            }
        )
        .reset_index(drop=True)
    )

    return pd.merge(workers, summary, how="left", on="workerId", validate="1:1")


def generate_static0(data):
    mean_shade_time = np.mean(
        data[data.shade > SHADE_CUTOFF].shadeTime
        / data[data.shade > SHADE_CUTOFF].shade
    )

    data["transfer"] = (
        100 * (data.bytesEnqueued + data.bytesDequeued) / AVERAGE_BANDWIDTH
    )

    data["compute"] = (
        data.apply(lambda x: calculate_compute_time(x, mean_shade_time), axis=1) / 1e7
    )

    data["weights"] = np.maximum(data.transfer, data.compute)
    data = data.sort_values("treeletId")
    weights = data.weights.values

    print(len(data))

    for i in range(len(weights)):
        print(f"{weights[i]:.8f} {1} {i}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"{sys.argv[0]} logs-dir")
        sys.exit(1)

    generate_static0(get_data(sys.argv[1]))
