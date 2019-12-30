#!/usr/bin/env python3

import os
import argparse
import subprocess

parser = argparse.ArgumentParser(description=(
    'Generate a CSV from worker *.INFO files'))

parser.add_argument('-i', '--info-directory', required=True,
                    help=(
                        'Path to directory containing .INFO files'))

parser.add_argument('-t', '--tag', default='RAY',
                    help=(
                        'Debug tag to select from .INFO files'))

parser.add_argument('-o', '--output-csv', required=True,
                    help=(
                        'Output csv location'))

args = parser.parse_args()

clen = 4 + len(args.tag)

subprocess.run("cat {firstfile} | grep \"\\[{tag}\\]\" | head -n1 |"
               "cut -c {clen}- > {ofile}".format(
                   firstfile=os.path.join(args.info_directory, "1.INFO"),
                   tag=args.tag, clen=clen, ofile=args.output_csv),
               shell=True, check=True)

subprocess.run("cat {fileglob} | grep \"\\[{tag}\\]\" | grep -v timestamp |"
               "cut -c {clen}- >> {ofile}".format(
                   fileglob=os.path.join(args.info_directory, "*.INFO"),
                   tag=args.tag, clen=clen, ofile=args.output_csv),
               shell=True, check=True)
