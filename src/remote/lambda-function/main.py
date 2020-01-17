import os
import sys
import shutil
import subprocess as sub

curdir = os.path.dirname(__file__)
sys.path.append(curdir)
os.environ['PATH'] = "{}:{}".format(curdir, os.environ.get('PATH', ''))

def run_command(command):
    completed = sub.run(command)
    return completed.returncode

def handler(event, context):
    storage_backend = event['storageBackend']
    coordinator_address = event['coordinator']
    coordinator_host, coordinator_port = event['coordinator'].split(':')

    # remove everything in the temp dir
    print('rm -rf /tmp/*: {}'.format(os.system("rm -rf /tmp/*")))

    command = ["pbrt-lambda-worker",
               "--ip", coordinator_host,
               "--port", coordinator_port,
               "--storage-backend", storage_backend]

    if event['samplesPerPixel']:
        command += ['--samples', str(event['samplesPerPixel'])]

    if event['maxPathDepth']:
        command += ['--max-depth', str(event['maxPathDepth'])]

    if event['rayLogRate']:
        command += ['--log-rays', str(event['rayLogRate'])]

    if event['bagLogRate']:
        command += ['--log-bags', str(event['bagLogRate'])]

    if event['directionalTreelets']:
        command += ['--directional']

    retcode = run_command(command)
    print("retcode={}".format(retcode))

    return {'stdout': retcode}
