import os
import sys
import shutil
import subprocess as sub

curdir = os.path.dirname(__file__)
sys.path.append(curdir)
os.environ['PATH'] = "{}:{}".format(curdir, os.environ.get('PATH', ''))

def run_command(command):
    try:
        output = sub.check_output(command, stderr=sub.STDOUT)
        return 0, output.decode('utf-8')
    except sub.CalledProcessError as exc:
        return exc.returncode, exc.output.decode('utf-8')

def handler(event, context):
    storage_backend = event['storageBackend']
    coordinator_address = event['coordinator']
    coordinator_host, coordinator_port = event['coordinator'].split(':')

    # remove old worker directories
    os.system("rm -rf /tmp/pbrt-worker.*")

    retcode, output = run_command(["pbrt-lambda-worker",
                                   coordinator_host,
                                   coordinator_port,
                                   storage_backend])

    print(output)
    print(retcode)

    return {'returnCode': retcode, 'stdout': output}
