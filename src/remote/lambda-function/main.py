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

    retcode = run_command(["pbrt-lambda-worker",
                           "--ip", coordinator_host,
                           "--port", coordinator_port,
                           "--storage-backend", storage_backend])

    print("retcode={}".format(retcode))

    return {'stdout': retcode}
