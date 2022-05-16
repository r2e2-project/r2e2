R2E2 &mdash; Really Elastic Ray Engine
===============

Building R2E2
-------------

To check out R2E2 together with all source dependencies, be sure to use the
`--recursive` flag when cloning the repository, i.e.,

```bash
$ git clone --recursive https://github.com/r2e2-project/r2e2
```

If you accidentally already cloned R2E2 without this flag (or to update an
R2E2 source tree after a new submodule has been added, run the following
command to also fetch the dependencies:

```bash
$ git submodule update --init --recursive
```

### Dependencies

Our version of R2E2 is dependent on the following libraries and tools (listed by their
apt package name):

* `g++`
* `gcc`
* `cmake`
* `protobuf-compiler`
* `libprotobuf-dev`
* `libssl-dev`
* `libunwind-dev`
* `liblzma-dev`
* `liblz4-dev`

Before building, you'll need to install each of these using your package manager.
On Ubuntu this is done by running something of the form:

```bash
$ sudo apt-get install <package-name>
```

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
