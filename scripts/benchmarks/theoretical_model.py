import pint
import math
U = pint.UnitRegistry()

treelet_size = 64 * U.megabytes

avg_ray_size = 500 * U.bytes

lambda_boot_time = 4 * U.seconds
lambda_bandwidth = 240 * U.Mbps

network_latency = 2 * U.milliseconds
time_to_process = 0

cost = 4.9e-5 / U.seconds

def prediction(scene_size, num_rays, path_length, num_workers, startup_time=True):
    scene_size = scene_size * U.gigabytes
    num_treelets = scene_size / treelet_size
    avg_treelets_per_ray = math.log(num_treelets)

    bytes_per_step = num_rays * avg_ray_size
    time_to_transmit = bytes_per_step / (lambda_bandwidth * num_workers)
    time_per_step = network_latency + time_to_transmit + time_to_process

    total_time = time_per_step * avg_treelets_per_ray * path_length * 2

    if startup_time:
        total_time += lambda_boot_time + treelet_size / lambda_bandwidth

    return total_time.to_base_units().to(U.seconds).magnitude
