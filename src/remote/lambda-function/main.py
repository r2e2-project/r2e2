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

    # remove old worker directories
    os.system("rm -rf /tmp/pbrt-worker.*")

    # remove old log files
    os.system("rm -rf /tmp/pbrt-lambda-*")

    command = ["pbrt-lambda-worker",
               "--ip", coordinator_host,
               "--port", coordinator_port,
               "--storage-backend", storage_backend,
               "--finished-ray", str(event.get('finishedRayAction', 0))]

    if event['sendReliably']:
        command += ['--reliable-udp']

    if event['samplesPerPixel']:
        command += ['--samples', str(event['samplesPerPixel'])]

    if event['rayActionsLogRate']:
        command += ['--log-rays', str(event['rayActionsLogRate'])]

    if event['packetsLogRate']:
        command += ['--log-packets', str(event['packetsLogRate'])]

    if event['maxUdpRate']:
        command += ['--max-udp-rate', str(event['maxUdpRate'])]

    if event['logLeases']:
        command += ['--log-leases']

    if event['collectDiagnostics']:
        command += ['--diagnostics']

    retcode = run_command(command)
    print("retcode={}".format(retcode))

    return {'stdout': retcode}
