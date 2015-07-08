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

WORKING_DIR=`sh bin/get-properties.sh kylin.hdfs.working.dir`
hadoop fs -mkdir -p $WORKING_DIR
if [ $? != 0 ]
then
    echo "failed to create $WORKING_DIR, Please make sure the user has right to access $WORKING_DIR"
    exit 1
fi
