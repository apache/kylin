#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

export KYLIN_HOME=/home/apache_kylin/kylin

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