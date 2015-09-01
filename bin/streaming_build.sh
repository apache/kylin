#!/bin/bash

source /etc/profile
source ~/.bash_profile

STREAMING=$1
INTERVAL=$2
DELAY=$3
MARGIN=$4
CURRENT_TIME_IN_SECOND=`date +%s`
CURRENT_TIME=$((CURRENT_TIME_IN_SECOND * 1000))
START=$(($CURRENT_TIME - CURRENT_TIME%INTERVAL - DELAY))
END=$(($CURRENT_TIME - CURRENT_TIME%INTERVAL - DELAY + INTERVAL))

ID="$START"_"$END"
echo "building for ${ID}" >> ${KYLIN_HOME}/logs/build_trace.log
sh ${KYLIN_HOME}/bin/kylin.sh streaming start ${STREAMING} ${ID} -oneoff true -start ${START} -end ${END} -streaming ${STREAMING} -margin ${MARGIN}