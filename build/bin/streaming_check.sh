#!/bin/bash

source /etc/profile
source ~/.bash_profile

receivers=$1
host=$2
tablename=$3
authorization=$4
projectname=$5
cubename=$6
sh ${KYLIN_HOME}/bin/kylin.sh monitor -receivers ${receivers} -host ${host} -tableName ${tablename} -authorization ${authorization} -cubeName ${cubename} -projectName ${projectname}