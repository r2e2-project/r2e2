#!/usr/bin/env python3

import sys
import argparse
import boto3
import paramiko
from botocore.exceptions import ClientError

parser = argparse.ArgumentParser()

parser.add_argument('-s', '--source', default='cloudrt-benchmark-scenes')
parser.add_argument('-d', '--destination', default='cloudrt-processed-scenes')
parser.add_argument('-n', '--instance-name', required=True)
parser.add_argument('-c', '--credentials', required=True)

args = parser.parse_args()

ec2 = boto3.client('ec2')
filters = [{
    'Name': 'tag:Name',
    'Values': [args.instance_name]
    }]

instance_info = ec2.describe_instances(Filters=filters)['Reservations'][0]['Instances'][0]
instance_id = instance_info['InstanceId']

try:
    ec2.start_instances(InstanceIds=[instance_id], DryRun=True)
except ClientError as e:
    if 'DryRunOperation' not in str(e):
        raise
try:
    response = ec2.start_instances(InstanceIds=[instance_id], DryRun=False)
except ClientError as e:
    print(e, file=sys.stderr)
    sys.exit(1)

boto3.resource('ec2').Instance(instance_id).wait_until_running()

key = paramiko.RSAKey.from_private_key_file(args.credentials)
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=instance_info['PublicIpAddress'], username='ubuntu', pkey=key)

def run_on_ec2(command):
    stdin, stdout, stderr = client.exec_command(command)
    return stdout.channel.recv_exit_status(), stdout.read(), stderr.read()

status, out, err = run_on_ec2("""
    cd ~/pbrt-v3
    git pull || exit 1
    cd build
    make || exit 1
    cd ../..
    screen -ls |grep dump-scenes && exit
    rm dump.log
    screen -L -Logfile ~/dump.log -S dump-scenes -d -m ~/pbrt-v3/scripts/benchmarks/process-benchmark-scenes.sh {source} {dest}
""".format(source=args.source, dest=args.destination))

if status is not 0:
    print(out)
    print(err)

print("done")

client.close()
