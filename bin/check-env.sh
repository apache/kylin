#!/bin/sh
#by source

if [ -z "$KYLIN_HOME" ]
then
    echo 'please make sure KYLIN_HOME has been set'
    exit 1
else
    echo "KYLIN_HOME is set to ${KYLIN_HOME}"
fi

if [ -z "$(command -v hbase version)" ]
then
    echo "Please make sure the user has the privilege to run hbase shell"
    exit 1
fi

if [ -z "$(command -v hive --version)" ]
then
    echo "Please make sure the user has the privilege to run hive shell"
    exit 1
fi

if [ -z "$(command -v hadoop version)" ]
then
    echo "Please make sure the user has the privilege to run hadoop shell"
    exit 1
fi
