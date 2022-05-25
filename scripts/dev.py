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

FILE_DIR = os.path.dirname(__file__)
SOURCE_DIR = os.path.join(FILE_DIR, '..')
DEFAULT_BUILD_DIR = 'build/'
DEFAULT_DIST_DIR = 'dist/'
DEFAULT_IMAGE_NAME = 'ghcr.io/r2e2-project/buildenv:latest'

run_docker_with_sudo = False


def run_docker_command(command):
    if run_docker_with_sudo:
        command = 'sudo ' + command

    print(f'+++ {command}', file=sys.stderr)

    sub.check_call(command, shell=True)


def requires_docker(func):

    def inner(*args, **kwargs):
        try:
            sub.check_call('docker --version', shell=True)
        except sub.CalledProcessError:
            sys.exit('Could not run `docker --version`. '
                     'Please make sure Docker is installed.')

        return func(*args, **kwargs)

    return inner


@requires_docker
def check_for_image(image_name):
    try:
        image_hash = sub.check_output(f'docker images -q {image_name}',
                                      shell=True).decode('utf-8').strip()
        if image_hash == '':
            sys.exit(f'Could not find the image "{image_name}". '
                     'Please build or pull the image first.')
    except sub.CalledProcessError:
        sys.exit(f'Could not run `docker images -q "{image_name}"`. '
                 'Please check your Docker installation.')

    print(f'>> Found the image "{image_name}" (hash={image_hash}).',
          file=sys.stderr)

    return image_hash


@requires_docker
def build_image(image_name, docker_file):
    print(f'>> Building image {image_name}...', file=sys.stderr)
    run_docker_command(f'docker build -t {image_name} '
                       f'-f "{docker_file}" '
                       f'{FILE_DIR}')


@requires_docker
def pull_image(image_name):
    print(f'>> Pulling image {image_name}...', file=sys.stderr)
    run_docker_command(f'docker pull {image_name}')


def build_project(image_name, build_dir, dist_dir):
    print(f'>> Building R2E2 (build_dir={build_dir}, dist_dir={dist_dir})...',
          file=sys.stderr)

    image_hash = check_for_image(image_name)

    print(
        f'>> Running the build script (build_dir={build_dir}, dist_dir={dist_dir})...',
        file=sys.stderr)

    os.makedirs(build_dir, exist_ok=True)
    os.makedirs(dist_dir, exist_ok=True)

    run_docker_command(
        f'docker run -it --rm '
        f'--user $(id -u):$(id -g) '
        f'--volume "{os.path.abspath(SOURCE_DIR)}":/r2e2 '
        f'--volume "{os.path.abspath(build_dir)}":/r2e2-build '
        f'--volume "{os.path.abspath(dist_dir)}":/r2e2-dist '
        f'{image_hash} '
        '/r2e2/scripts/build_inside_container.sh /r2e2 /r2e2-build /r2e2-dist '
    )


def main():
    global run_docker_with_sudo

    parser = argparse.ArgumentParser(
        description=
        'Build R2E2 binaries, prepare the bucket, and install Lambda functions.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # MAIN
    parser.add_argument('--build-dir',
                        help='path to build directory',
                        default=DEFAULT_BUILD_DIR)

    parser.add_argument('--dist-dir',
                        help='path to dist directory',
                        default=DEFAULT_DIST_DIR)

    parser.add_argument('--image-name',
                        help='name of the build environment docker image',
                        default=DEFAULT_IMAGE_NAME)  

    parser.add_argument('--sudo',
                        help='run docker commands with sudo',
                        action='store_true')

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

    # MAIN -> 'build'
    parser_build = subparsers.add_parser(
        'build', help='building R2E2 binaries inside the image')

    args = parser.parse_args()
    run_docker_with_sudo = args.sudo

    if args.action == 'image':
        if args.image_action == 'build':
            build_image(args.image_name, args.dockerfile)
        elif args.image_action == 'pull':
            pull_image(args.image)
    elif args.action == 'build':
        build_project(args.image_name, args.build_dir, args.dist_dir)


if __name__ == '__main__':
    main()
