#!/bin/bash

if ! grep -q "PULLED" ~/status; then
    cd ~/pbrt-v3
    git pull || exit 1
    cd build
    make || exit 1
    cd ../..
    echo "PULLED" >> ~/status
fi

if ! grep -q "SYNCED" ~/status; then
    aws s3 sync s3://$1 ~/benchmark-scenes || exit 1
    echo "SYNCED" >> ~/status
fi

mkdir -p ~/dump-scenes
while read scene; do
    split=(${scene})
    grep -q ${split[0]} ~/status && continue

    dir=~/dump-scenes/${split[0]}
    rm -rf $dir
    mkdir $dir
    echo "~/pbrt-v3/build/pbrt --dumpscene $dir --nomaterial ~/benchmark-scenes/${split[1]}"
    ~/pbrt-v3/build/pbrt --dumpscene $dir --nomaterial ~/benchmark-scenes/${split[1]} || exit 1

    echo "${split[0]}" >> ~/status
done < ~/benchmark-scenes/index

# Push processed scenes
aws s3 sync dump-scenes/ s3://$2

rm status

exit 0
