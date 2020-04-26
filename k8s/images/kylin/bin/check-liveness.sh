#!/bin/bash
result="$(curl --write-out %{http_code} --silent --output /dev/null http://127.0.0.1:7070/kylin/)"
if [ $result == 200 ]
then
  echo "check http get successful"
  exit 0
else
  echo "check http get failed"
  exit 1
fi
