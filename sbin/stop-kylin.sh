#!/bin/sh

ps -fu $USER | grep tomcat | grep -v "grep" | awk '{print $2}' | xargs kill
echo "all tomcats started by $USER are killed"