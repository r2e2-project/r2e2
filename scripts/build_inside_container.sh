#!/bin/bash -exu

if [ "$#" -ne 3 ]; then
  echo "$0 SRC-DIR BUILD-DIR DIST-DIR"
  exit 1
fi

SRC_DIR=$1
BUILD_DIR=$2
DIST_DIR=$3

test -d ${SRC_DIR}
test -d ${BUILD_DIR}
test -d ${DIST_DIR}

cmake3 -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DIST_DIR} \
       -S ${SRC_DIR} -B ${BUILD_DIR}

cd ${BUILD_DIR}
make -j$(nproc) r2e2-lambda-{worker,master}
cp r2e2-lambda-{master,worker} ${DIST_DIR}
