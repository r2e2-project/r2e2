#!/usr/bin/env python3

import resource
import argparse
import subprocess
import sys
import os
import shutil
import signal
import asyncio
import json

from math import floor, log10
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--samples', nargs='+', type=int)
parser.add_argument('-l', '--lambdas', nargs='+', type=int, required=True)
parser.add_argument('-S', '--s3-path', default='cloudrt-processed-scenes')
parser.add_argument('-b', '--build-path', required=True)
parser.add_argument('-x', '--exclude', nargs='+', type=str)
parser.add_argument('-i', '--include', nargs='+', type=str)
parser.add_argument('-I', '--ip')
parser.add_argument('-g', '--generate-static', action='store_true')
parser.add_argument('-p', '--start-port', default=9900, type=int)
parser.add_argument('-o', '--out-dir', required=True)
parser.add_argument('-n', '--run-name', required=True, type=str)
parser.add_argument('-D', '--directional', action='store_true')
parser.add_argument('-F', '--install-function', action='store_true')

args = parser.parse_args()

resource.setrlimit(resource.RLIMIT_NOFILE,
        (65536, 65536))

pbrt_path = os.path.abspath(os.path.join(os.path.dirname(
    os.path.realpath(__file__)),
    os.path.join(os.path.pardir, os.path.pardir)))

pbrt_scripts_path = os.path.join(pbrt_path, 'scripts')

build_path = os.path.abspath(args.build_path)

master_path = os.path.join(build_path, 'pbrt-lambda-master')
worker_path = os.path.join(build_path, 'pbrt-lambda-worker')

if args.install_function:
    subprocess.run("""
        ./create-function.py --pbrt-lambda-worker {worker_path} --delete
        """.format(worker_path=worker_path),
        cwd=os.path.join(pbrt_path, 'src/remote'), check=True, shell=True)

if args.ip is None:
    ip = subprocess.check_output("curl -s http://checkip.amazonaws.com",
                                 shell=True).decode('utf-8').strip()
else:
    ip = args.ip

scenes = subprocess.check_output("""
    aws s3 ls s3://{s3_path}|awk -F' ' '{{print $2}}'|cut -d/ -f1
    """.format(s3_path=args.s3_path), shell=True).decode('utf-8').split()

if len(args.include) > 0:
    scenes = list(set(scenes).intersection(set(args.include)))
    if len(scenes) < len(args.include):
        print("You defined missing scenes", file=sys.stderr)
        sys.exit(1)
else:
    scenes = list(set(scenes) - set(args.exclude))

def ignore_ctrlc(sig, frame):
    return
signal.signal(signal.SIGINT, ignore_ctrlc)

samples = args.samples if args.samples else [1]

out_dir = os.path.join(args.out_dir, args.run_name + datetime.now().strftime('-%H-%M-%S-%m-%d-%Y'))
os.makedirs(out_dir)

cmds = []
cur_port = args.start_port
for i, scene in enumerate(scenes):
    cmdprefix = ("{master_path} --ip {ip} --timeout 60"
              " --storage-backend s3://{s3_path}/{scene}?region=us-west-2"
              " --aws-region us-west-2 -T auto -w 2 -d".format(
                  master_path=master_path,
                  ip=ip,
                  s3_path=args.s3_path,
                  scene=scene))

    if args.generate_static:
        cmdprefix += " -a uniform"
    else:
        cmdprefix += " -a static"

    if args.directional:
        cmdprefix += ' --directional'

    for nlambdas in args.lambdas:
        for spp in samples:
            onespp_rate = 1 / spp / 10
            dir = os.path.join(out_dir,
                    "{scene}-{nlambdas}-{spp}".format(scene=scene,
                                                      nlambdas=nlambdas,
                                                      spp=spp))
            cmd = cmdprefix + (" -S {spp} -D {dir} --job-summary {json}"
                               " --lambdas {nlambdas} --port {port}").format(
                    spp=spp,
                    dir=dir,
                    nlambdas=nlambdas,
                    port=cur_port,
                    json=os.path.join(dir, "master.json"))
            cur_port += 1
            cmds.append((cmd, dir, scene, nlambdas, spp))

#async def launch(cmd):
#    print(cmd)
#    proc = await asyncio.create_subprocess_shell(cmd,
#        stdout=asyncio.subprocess.PIPE,
#        stderr=asyncio.subprocess.PIPE)
#
#    stdout, stderr = await proc.communicate()

#async def run(cmd_list):
#    await asyncio.gather(*[launch(cmd) for cmd, dir, scene in cmd_list])
#
#asyncio.run(run(cmds))

def job_finished(master):
    with open(master, 'r') as f:
        info = json.load(f)
    return int(info['finishedPaths']) > 0 and int(info['finishedPaths']) == int(info['totalPaths'])

def launch(cmd):
    subprocess.run(cmd, shell=True)

for cmd, dir, scene, nlambdas, spp in cmds:
    master = os.path.join(dir, 'master.json')
    i = 0

    while not os.path.isfile(master) or not job_finished(master):
        shutil.rmtree(dir, ignore_errors=True)
        if i > 2:
            break
        os.mkdir(dir)

        with open(os.path.join(dir, 'COMMAND'), 'w') as fout:
            print(cmd, file=fout)

        launch(cmd)
        i += 1

perfs = {}

cwd = os.getcwd()
for cmd, dir, scene, nlambdas, spp in cmds:
    if not os.path.isdir(dir):
        continue

    os.chdir(dir)

    if args.generate_static:
        subprocess.run(os.path.join(pbrt_scripts_path,
            "generate_static_assignment.sh") + " treelets.csv > STATIC0",
            shell=True, check=True)
        subprocess.run("aws s3 cp STATIC0 s3://{s3_path}/{scene}/".format(
            s3_path=args.s3_path, scene=scene), shell=True, check=True)
    else:
        graph_title = "{scene}: {nworkers} - {spp}spp".format(scene=scene, nworkers=nlambdas, spp=spp)
        os.mkdir('graphs')
        subprocess.run(os.path.join(os.path.join(pbrt_scripts_path, 'benchmarks'),
            "detail-graphs.py") + " -i . -o graphs -t \"{title}\"".format(title=graph_title),
            shell=True, check=True)

    os.chdir(cwd)

    with open(os.path.join(dir, 'master.json')) as f:
        info = json.load(f)

    perfs[scene] = int(info['finishedRays']) / info['numLambdas'] / info['rayTime']

with open(os.path.join(out_dir, 'perf.json'), 'w') as f:
    json.dump(perfs, f)
