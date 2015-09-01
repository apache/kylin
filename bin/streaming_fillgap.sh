#!/bin/bash

source /etc/profile
source ~/.bash_profile

streaming=$1
margin=$2

cd ${KYLIN_HOME}
sh ${KYLIN_HOME}/bin/kylin.sh streaming start ${streaming} fillgap -streaming ${streaming} -fillGap true -margin ${margin}