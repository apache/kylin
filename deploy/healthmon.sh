#!/usr/bin/env bash

curl --max-time 20 \
	--data '{"sql":"select count(*) from kylin_fact","offset":0,"limit":50000,"acceptPartial":true,"project":"Default"}' \
	-H "Authorization:Basic eWFuZ2xpOTpQYSQkdzRyZA==" \
	-H "Content-Type:application/json;charset=UTF-8" \
	http://localhost:7070/kylin/api/query \
	>/dev/null

if [ "$?" == "0" ]; then
	echo "Good."
else
	echo "Bad."
	TS_FILE=/tmp/kylin_healthmon_ts
	LAST_TS=`stat -c%Y $TS_FILE 2>/dev/null`
	CURR_TS=`date +%s`
	echo last: $LAST_TS
	echo curr: $CURR_TS
	if (( ${LAST_TS:-"0"} < $CURR_TS - 3600 )); then
		echo "send mail"
		if [ "$?" == "0" ]; then
			touch $TS_FILE
		fi
	fi
fi
