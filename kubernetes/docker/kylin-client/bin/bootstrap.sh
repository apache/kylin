#!/bin/bash

# enable cron job
sudo crond -i -p

#sleep 60 second due to kite enable need more time
sleep 60

if [[ $1 == "server" ]]; then
  $KYLIN_HOME/bin/kylin.sh start
elif [[ $1 == "streaming" ]]; then
  $KYLIN_HOME/bin/kylin.sh streaming start
fi

if [[ $2 == "-d" ]]; then
  while true; do sleep 3000; done
fi
