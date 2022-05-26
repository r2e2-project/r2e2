R2E2 &mdash; Really Elastic Ray Engine
===============

Building R2E2
-------------

>⚠️ The following instructions have only been tested on the newest version of
Ubuntu.

### Get the source

To check out R2E2 together with all source dependencies, be sure to use the
`--recursive` flag when cloning the repository, i.e.,

```bash
$ git clone --recursive https://github.com/r2e2-project/r2e2
```

If you accidentally already cloned R2E2 without this flag (or to update an
R2E2 source tree after a new submodule has been added), run the following
command to also fetch the dependencies:

```bash
$ git submodule update --init --recursive
```

### Route A: Docker

1. Make sure you have [Docker](https://docs.docker.com/engine/install/)
installed. In addition, you will need `python3`.

>❗ By default, the provided scripts **doesn't** run `docker` with `sudo`. This
requires your current user to be a member of `docker` group. If you want to run
it with `sudo`, pass `--sudo` to the script.


2. Create two empty directories (anywhere on your machine):
   * one for build artifacts (defaults to `build/`)
   * one for outputs (defaults to `dist/`)

3. Create the build environment container image by running the following
command (which will build [this Dockerfile](scripts/buildenv.Dockerfile)).

```bash
./scripts/dev.py image build
```

Alternatively, you can pull [the
image](https://github.com/orgs/r2e2-project/packages/container/package/buildenv)
from our container repository by running

```bash
./scripts/dev.py image pull
```

4. Build the project by running
```bash
./scripts/dev.py build --build-dir <PATH-TO-BUILD-DIR>
                       --dist-dir <PATH-TO-DIST-DIR>
```


### Route B: Building from scratch

#### Dependencies

Our version of R2E2 is dependent on the following libraries and tools (listed by their
Ubuntu package name):

* `python3`
* `cmake` >= 3.0.0
* `g++` >= 9.0.0
* `gcc` >= 9.0.0
* `pkg-config`
* `zlib1g-dev`
* `protobuf-compiler` >= 3.0.0
* `libprotobuf-dev` >= 3.0.0
* `libgoogle-perftools-dev` (for [`tcmalloc`](https://github.com/google/tcmalloc))
* `libssl-dev`
* `libunwind-dev`
* `liblzma-dev`
* `liblz4-dev`

Before building, you'll need to install each of these using your package manager.
On Ubuntu this is done by running something of the form:

```bash
$ sudo apt-get install cmake gcc g++ pkg-config zlib1g-dev protobuf-compiler \
                       libprotobuf-dev libgoogle-perftools-dev libssl-dev \
                       libunwind-dev liblzma-dev liblz4-dev python3
```

#### Building R2E2

Build R2E2 like a normal CMake project.

```
cd r2e2
mkdir build
cd build
cmake ..
make -j$(nproc)
```

>❗ You may have problems running the worker binary on AWS Lambda. In that case,
>you need to go down the Docker road. Our experience is that some static binaries
>built on recent versions of Ubuntu doesn't run in Lambda environment.

Running R2E2
------------------------

### Environmental Variables

Before running the distributed version of R2E2, you must have the following
environmental variables set:

   * `AWS_ACCESS_KEY_ID`
   * `AWS_SECRET_ACCESS_KEY`
   * `AWS_REGION`
   * `R2E2_LAMBDA_ROLE`

### Network

Your machine must have a public IP.

### Storage

You must (in advance) create a R2E2 dump of a scene, and copy that dump to an S3 bucket.

You can copy folders to buckets using `aws s3 cp --recursive <path-to-folder>
s3://<s3-bucket-name>`

### Runnning

Distributed R2E2 has two programs, a master and a worker. The master can be invoked as

```
r2e2-lambda-master --scene-path <path-to-pbrt-scene-dump> \
                   --ip <public-ip-of-machine> \
                   --port <port> \
                   --lambdas <number-of-workers> \
                   --storage-backend s3://<s3-bucket-name>?region=<aws-region> \
                   --aws-region <aws-region>
```

And the worker as

```
r2e2-lambda-worker --ip <master-ip> \
                   --port <master-port> \
                   --storage-backend s3://<s3-bucket-name>?region=<aws-region>
```

You may actually run all of these locally! However, by setting
<number-of-workers> to be greater than 0, the master will fire up lambda
instances running the worker program.

The master also support a few important options:

```
  -t --treelet-stats         show treelet use stats
  -w --worker-stats          show worker use stats
  -d --diagnostics           collect & display diagnostics
  -a --assignment TYPE       indicate assignment type:
                             * static
                             * uniform (default)
```

### Changing the worker binary

To change the binary that the AWS Lambda workers run, you must execute:

```
./src/remote/create-function.py --r2e2-lambda-worker <path-to-r2e2-lambda-worker> --delete
```
