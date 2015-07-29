#!/bin/bash

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
