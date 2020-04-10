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

    command = ["r2t2-lambda-worker",
               "--ip", coordinator_host,
               "--port", coordinator_port,
               "--storage-backend", storage_backend]

    if event['samplesPerPixel']:
        command += ['--samples', str(event['samplesPerPixel'])]

    if event['maxPathDepth']:
        command += ['--max-depth', str(event['maxPathDepth'])]

    if event['baggingDelay']:
        command += ['--bagging-delay', str(event['baggingDelay'])]

    if event['rayLogRate']:
        command += ['--log-rays', str(event['rayLogRate'])]

    if event['bagLogRate']:
        command += ['--log-bags', str(event['bagLogRate'])]

    if event['directionalTreelets']:
        command += ['--directional']

    for server in event.get('memcachedServers', []):
        command += ['--memcached-server', server]

    retcode = run_command(command)
    print("retcode={}".format(retcode))

    return {'stdout': retcode}
