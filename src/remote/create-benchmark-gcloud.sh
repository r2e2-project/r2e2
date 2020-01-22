#!/bin/bash -e

FUNCTION_NAME=pbrt-gcloud-function
FUNCTION_PATH=benchmark-gcloud-function
REGION=us-central1
MEMORY=2048
TIMEOUT=540

cp ../../build/benchmark-worker ${FUNCTION_PATH}

gcloud functions deploy --region=${REGION} --memory ${MEMORY} ${FUNCTION_NAME} --trigger-http --quiet --runtime=python37 --source=${FUNCTION_PATH} --timeout=${TIMEOUT} --format=json --entry-point handler --set-env-vars=AWS_ACCESS_KEY_ID=TEMP,AWS_SECRET_ACCESS_KEY=TEMP

rm ${FUNCTION_PATH}/benchmark-worker

echo
echo "export GG_GCLOUD_FUNCTION=[https-trigger-url]"
