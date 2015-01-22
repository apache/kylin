#!/bin/sh

if [ -z "$CATALINA_HOME" ]
then
    echo "Please set CATALINA_HOME so that Kylin knows where to start tomcat"
    exit 1
else
    echo "CATALINA_HOME is set to $CATALINA_HOME"
fi

if [ "$1" = "start" ] ; then
        export HBASE_CLASSPATH_PREFIX=/etc/kylin:${CATALINA_HOME}/bin/bootstrap.jar:${CATALINA_HOME}/bin/tomcat-juli.jar:${CATALINA_HOME}/lib/*:$HBASE_CLASSPATH_PREFIX
        hbase -Djava.util.logging.config.file=${CATALINA_HOME}/conf/logging.properties -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true -Dspring.profiles.active=sandbox -Djava.endorsed.dirs=${CATALINA_HOME}/endorsed  -Dcatalina.base=${CATALINA_HOME} -Dcatalina.home=${CATALINA_HOME} -Djava.io.tmpdir=${CATALINA_HOME}/temp  org.apache.hadoop.util.RunJar ${CATALINA_HOME}/bin/bootstrap.jar  org.apache.catalina.startup.Bootstrap start > ${CATALINA_HOME}/logs/kylin_sandbox.log 2>&1 &
        echo "A new tomcat is started by $USER, stop it using kylin.sh stop"
        echo "Please visit http://<your_sandbox_ip>:7070/kylin to play with the cubes! (Useranme: ADMIN, Password: KYLIN)"

elif [ "$1" = "stop" ]; then
        # kill all tomcats started by current user
        ps -fu $USER | grep tomcat | grep -v "grep" | awk '{print $2}' | xargs kill
        echo "all tomcats started by $USER are killed"

else
        echo "Usage: kylin.sh start, or kylin.sh stop"

fi


