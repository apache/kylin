#!/bin/bash

# uncomment following to for it to take effect
# export KYLIN_JVM_SETTINGS="-Xms16g -Xmx16g -XX:MaxPermSize=512m -XX:NewSize=3g -XX:MaxNewSize=3g -XX:SurvivorRatio=4 -XX:+CMSClassUnloadingEnabled -XX:+CMSParallelRemarkEnabled -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:CMSInitiatingOccupancyFraction=70 -XX:+DisableExplicitGC"

# uncomment following to for it to take effect
# export KYLIN_DEBUG_SETTINGS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -javaagent:${KYLIN_HOME}/lib/CustomAgent.jar -Dcom.ebay.lnp.rmiagent.port=8080"

# uncomment following to for it to take effect
# export KYLIN_LD_LIBRARY_SETTINGS="-Djava.library.path=/apache/hadoop/lib/native/Linux-amd64-64"

export KYLIN_EXTRA_START_OPTS=""

if [ ! -z "${KYLIN_JVM_SETTINGS}" ]
then
    echo "KYLIN_JVM_SETTINGS is ${KYLIN_JVM_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_JVM_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    echo "KYLIN_JVM_SETTINGS is not set, using default jvm settings"
fi

if [ ! -z "${KYLIN_DEBUG_SETTINGS}" ]
then
    echo "KYLIN_DEBUG_SETTINGS is ${KYLIN_DEBUG_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_DEBUG_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    echo "KYLIN_DEBUG_SETTINGS is not set, will not enable remote debuging"
fi

if [ ! -z "${KYLIN_LD_LIBRARY_SETTINGS}" ]
then
    echo "KYLIN_LD_LIBRARY_SETTINGS is ${KYLIN_LD_LIBRARY_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_LD_LIBRARY_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    echo "KYLIN_LD_LIBRARY_SETTINGS is not set, lzo compression at MR and hbase might not work"
fi
