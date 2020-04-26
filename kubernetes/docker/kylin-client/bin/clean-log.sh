#!/bin/bash

export KYLIN_HOME=/home/b_kylin/kylin2

# Rotate kylin out
timestamp=`date +%Y-%m-%d`
mv ${KYLIN_HOME}/logs/kylin.out ${KYLIN_HOME}/logs/kylin.out.$timestamp
mv /tmp/cron_b_kylin.out /tmp/cron_b_kylin.out.$timestamp

# Delete kylin log before 3 days
find ${KYLIN_HOME}/logs  -mtime +2 -type f -delete

# Delete kylin tomcat log before 3 days
find ${KYLIN_HOME}/tomcat/logs -mtime +2 -type f -delete

# Delete keytab log before 3 days
find /tmp/ -name 'cron_b_kylin.out.*' -mtime +2 -type f -delete

# Delete hive log and temp files
find /tmp/ -name '*_resources' -mtime +1 -type d -exec rm -rf {} +
find /tmp/ -name 'hadoop-unjar*' -mtime +1 -type d -exec rm -rf {} +
find /tmp/b_kylin/ -mtime +1 -type f -delete
find /tmp/b_kylin/ -mtime +1 -type d -exec rm -rf {} +