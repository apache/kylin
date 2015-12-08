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

ALERT="your@email.com"

OUTPUT=$(
	curl --max-time 20 -# \
	--data '{"sql":"select count(*) from test_kylin_fact","offset":0,"limit":50000,"acceptPartial":true,"project":"default"}' \
	-H "Authorization:Basic QURNSU46S1lMSU4=" \
	-H "Content-Type:application/json;charset=UTF-8" \
	http://localhost:7070/kylin/api/query \
)

# ----------------------------------------------------------------------------

date

if [[ $OUTPUT == *"results"* ]]; then
	echo "Good."
else
	echo "Bad."
	TS_FILE=/tmp/kylin_healthmon_ts
	LAST_TS=`stat -c%Y $TS_FILE 2>/dev/null`
	CURR_TS=`date +%s`
	echo last: $LAST_TS
	echo curr: $CURR_TS
	if (( ${LAST_TS:-"0"} < $CURR_TS - 3600 )); then
		echo "Sending mail..."
		echo "Kylin Prod health check failed as of $(date)." | mail -s "KYLIN PROD DOWN" $ALERT
		if [ "$?" == "0" ]; then
			touch $TS_FILE
		fi
	fi
fi
