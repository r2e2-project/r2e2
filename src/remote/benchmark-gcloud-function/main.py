import os
import sys
import shutil
import subprocess as sub
import json

curdir = os.path.dirname(__file__)
sys.path.append(curdir)
os.environ['PATH'] = "{}:{}".format(curdir, os.environ.get('PATH', ''))

def handler(request):
    event = request.get_json()

    command = ["./benchmark-worker",
               event['workerId'], event['storageUri'],
               event['bagSize'], event['threads'],
               event['duration'], event['send'],
               event['recv']]

    command = [str(x) for x in command]

    output = sub.check_output(command).decode('ascii')

    return json.dumps({'output': output})
