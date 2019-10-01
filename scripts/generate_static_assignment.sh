#!/bin/bash -e

if [[ $# != 1 ]]; then
    echo "USAGE: $0 <data.csv>"
fi

if [[ ! $(command -v q) ]]; then
    echo "'q' (http://harelba.github.io/q/) is not installed."
    exit 1
fi

DATA_PATH=$1

TREELETS=$(q -H -d, "SELECT treeletID, CAST(SUM(raysProcessed) AS INT64) FROM ${DATA_PATH} GROUP BY treeletID ORDER BY treeletID")

TREELET_COUNT=$(echo "$TREELETS" | wc -l)

echo ${TREELET_COUNT}

while read line
do
    IFS=',' read -ra INFO <<< "$line"
    echo "${INFO[1]} 1 ${INFO[0]}"
done <<< "${TREELETS}"
