import os
import sys
import shutil
import subprocess as sub

curdir = os.path.dirname(__file__)
sys.path.append(curdir)
os.environ['PATH'] = "{}:{}".format(curdir, os.environ.get('PATH', ''))

def handler(event, context):
    command = ["benchmark-worker",
               event['workerId'], event['storageUri'], event['bagSize'], event['threads'], event['duration'], event['send'],
               event['recv']]

    for s in event.get('memcachedServers', []):
        command += [s]

    command = [str(x) for x in command]

    pipes = sub.Popen(command, stdout=sub.PIPE, stderr=sub.PIPE)
    stdout, stderr = pipes.communicate()

    output = stdout.decode('ascii')
    error = stderr.decode('utf-8')

    return {'output': output, 'error': error, 'command': " ".join(command), 'retcode': pipes.returncode}
