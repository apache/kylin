#!/bin/sh

echo "Checking KYLIN_HOME..."
if [ -z "$KYLIN_HOME" ]
then
    echo 'please make sure KYLIN_HOME has been set'
    exit 1
else
    echo "KYLIN_HOME is set to ${KYLIN_HOME}"
fi

echo "Checking hbase..."
if [ -z "$(command -v hbase version)" ]
then
    echo "Please make sure the user has the privilege to run hbase shell"
    exit 1
else
    echo "hbase check passed"
fi

echo "Checking hive..."
if [ -z "$(command -v hive --version)" ]
then
    echo "Please make sure the user has the privilege to run hive shell"
    exit 1
else
    echo "hive check passed"
fi

echo "Checking hadoop..."
if [ -z "$(command -v hadoop version)" ]
then
    echo "Please make sure the user has the privilege to run hadoop shell"
    exit 1
else
    echo "hadoop check passed"
fi

exit 0