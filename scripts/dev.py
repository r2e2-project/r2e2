#!/usr/bin/env python3

import sys

MIN_PYTHON = (3, 6)
if sys.version_info < MIN_PYTHON:
    sys.exit('Python %s.%s or later is required.\n' % MIN_PYTHON)

import os
import shutil
import pprint
import argparse
import subprocess as sub

BUILD_DIR = 'build/'
FILE_DIR = os.path.dirname(__file__)
DEFAULT_IMAGE_NAME = 'ghcr.io/r2e2-project/buildenv:latest'


def requires_docker(func):

    def inner(*args, **kwargs):
        try:
            sub.check_output('sudo docker --version', shell=True)
        except sub.CalledProcessError as ex:
            sys.exit('Could not run `sudo docker --version`. '
                     'Please make sure Docker is installed.')

        return func(*args, **kwargs)

    return inner


def run_command(command):
    sub.check_call(command, stdin=sub.DEVNULL, shell=True)


@requires_docker
def build_image(image_name, docker_file):
    print(f'>> Building image {image_name}...', file=sys.stderr)
    run_command(f'sudo docker build -t {image_name} '
                f'-f "{docker_file}" '
                f'{FILE_DIR}')


@requires_docker
def pull_image(image_name):
    print(f'>> Pulling image {image_name}...', file=sys.stderr)
    run_command(f'sudo docker pull {image_name}')


def main():
    parser = argparse.ArgumentParser(
        description=
        'Build R2E2 binaries, prepare the bucket, and install Lambda functions.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # MAIN
    parser.add_argument('--dir', help='build directory', default=BUILD_DIR)
    subparsers = parser.add_subparsers(dest='action')

    # MAIN -> 'image'
    parser_image = subparsers.add_parser(
        'image',
        help="preparing the build environment image",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser_image_subparsers = parser_image.add_subparsers(dest='image_action')

    # MAIN -> 'image' -> 'build'
    parser_image_build = parser_image_subparsers.add_parser(
        'build',
        help='build the image from a Dockerfile',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser_image_build.add_argument('--dockerfile',
                                    help='Dockerfile for the image',
                                    default=os.path.join(
                                        FILE_DIR, "buildenv.Dockerfile"))

    # MAIN -> 'image' -> 'pull'
    parser_image_pull = parser_image_subparsers.add_parser(
        'pull',
        help='pull the image from the container registry',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser_image.add_argument('--image-name',
                              help='name of the docker image',
                              default=DEFAULT_IMAGE_NAME)

    parser_binaries = subparsers.add_parser(
        'binaries', help='building R2E2 binaries inside the image')

    args = parser.parse_args()
    pprint.pprint(args)

    if args.action == 'image':
        if args.image_action == 'build':
            build_image(args.image_name, args.dockerfile)
        elif args.image_action == 'pull':
            pull_image(args.image)


if __name__ == '__main__':
    main()
