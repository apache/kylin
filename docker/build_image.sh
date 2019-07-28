#!/usr/bin/env bash

echo "start build kylin image base on current source code"

rm -rf ./kylin
mkdir -p ./kylin

echo "start copy kylin source code"

for file in `ls ../../kylin/`
do
    if [ docker != $file ]
    then
        cp -r ../../kylin/$file ./kylin/
    fi
done

echo "finish copy kylin source code"

docker build -t apache-kylin-standalone .