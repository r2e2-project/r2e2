#!/bin/bash -exu

if [[ $# != 2 ]]; then
  echo "Usage: $(basename $0) <agg-program> <log-dir>"
  exit 1
fi

AGG_PROGRAM=$1
LOGDIR=$2

INFO=${LOGDIR}/info.json
JOB_ID=$(cat ${INFO} | jq -r .jobId)

SAMPLES_DIR=$(mktemp -d -t -p ${LOGDIR} samples-${JOB_ID}.XXXX)
TEMP_SCENE_DIR=$(mktemp -d -t -p ${LOGDIR} scene-${JOB_ID}.XXXX)
trap "rm -rf '${SAMPLES_DIR}' '${TEMP_SCENE_DIR}'" EXIT

CAMERA=$(cat ${INFO} | jq -r .altSceneObjects.CAMERA)

if [[ $CAMERA == "null" ]]; then
  CAMERA=CAMERA
fi

SCENE_URI=$(cat ${INFO} | jq -r .storageBackend)
SCENE_BUCKET_BASE=$(dirname ${SCENE_URI})
SCENE_NAME=$(basename ${SCENE_URI} | awk -F\? '{ print $1 }')

# 1) download the scene
for x in {MANIFEST,LIGHTS,SCENE,SAMPLER,${CAMERA}}
do
  aws s3 cp ${SCENE_BUCKET_BASE}/${SCENE_NAME}/${x} "${TEMP_SCENE_DIR}"
done

# 2) rename the camera
mv "${TEMP_SCENE_DIR}"/${CAMERA} "${TEMP_SCENE_DIR}"/CAMERA

# 3) download the samples
aws s3 cp --recursive ${SCENE_BUCKET_BASE}/jobs/${JOB_ID}/samples/ "${SAMPLES_DIR}"

# 4) aggregate
find "${SAMPLES_DIR}" -type f -name 'B*' | "${AGG_PROGRAM}" "${TEMP_SCENE_DIR}"
