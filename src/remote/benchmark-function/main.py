import os
import sys
import shutil
import subprocess as sub

curdir = os.path.dirname(__file__)
sys.path.append(curdir)
os.environ['PATH'] = "{}:{}".format(curdir, os.environ.get('PATH', ''))

def handler(event, context):
    command = ["benchmark-worker",
               event['workerId'], event['storageUri'], event['bagSize'], event['threads'], event['duration']]

    command = [str(x) for x in command]

    output = sub.check_output(command)

    return {'output': output}
