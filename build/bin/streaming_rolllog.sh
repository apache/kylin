#!/bin/bash

source /etc/profile
source ~/.bash_profile

KYLIN_LOG_HOME=${KYLIN_HOME}/logs
cd ${KYLIN_LOG_HOME}
timestamp=`date +%Y_%m_%d_%H_%M_%S`
tarfile=logs_archived_at_${timestamp}.tar
files=`find -L . ! -name '*.tar' -type f -mtime +1` # keep two days' log
echo ${files} | xargs tar -cvf ${tarfile}
echo ${files} | xargs rm