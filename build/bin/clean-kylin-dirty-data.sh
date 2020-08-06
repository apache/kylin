#!/bin/bash
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/set-kylin-home.sh $@

today=`date +"%Y-%m-%d"`
before7day=`date -d "-7 day" +"%Y-%m-%d"`

echo "*******************start clean metastore***************************"
${KYLIN_HOME}/bin/metastore.sh clean --delete true --jobThreshold 91 >> ${KYLIN_HOME}/logs/clean-kylin-dirty-data.log.$today 2>&1

echo "*******************start clean storage***************************"
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true >> ${KYLIN_HOME}/logs/clean-kylin-dirty-data.log.$today 2>&1
#clean before 7 day log
before7dayfile=${KYLIN_HOME}/logs/clean-kylin-dirty-data.log.$before7day
if [ -f "$before7dayfile" ]; then
  rm $before7dayfile
fi
