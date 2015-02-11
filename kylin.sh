#!/bin/bash

if [ -z "$CATALINA_HOME" ]
then
    echo "Please set CATALINA_HOME so that Kylin knows where to start tomcat"
    exit 1
else
    echo "CATALINA_HOME is set to $CATALINA_HOME"
fi

if [ ! -z "$KYLIN_LD_LIBRARY_PATH" ]
then
    echo "KYLIN_LD_LIBRARY_PATH is set to $KYLIN_LD_LIBRARY_PATH"
fi

#The location of all hadoop/hbase configurations are difficult to get.
#Plus, some of the system properties are secretly set in hadoop/hbase shell command.
#For example, in hdp 2.2, there is a system property called hdp.version,
#which we cannot get until running hbase or hadoop shell command.
#
#To save all these troubles, we use hbase runjar to start tomcat.
#In this way we no longer need to explicitly configure hadoop/hbase related classpath for tomcat,
#hbase command will do all the dirty tasks for us:
if [ "$1" = "start" ] ; then
        export HBASE_CLASSPATH_PREFIX=/etc/kylin:${CATALINA_HOME}/bin/bootstrap.jar:${CATALINA_HOME}/bin/tomcat-juli.jar:${CATALINA_HOME}/lib/*:$HBASE_CLASSPATH_PREFIX
        hbase  -XX:PermSize=256M -XX:MaxPermSize=512M -Djava.util.logging.config.file=${CATALINA_HOME}/conf/logging.properties -Djava.library.path=${KYLIN_LD_LIBRARY_PATH} -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true -Dspring.profiles.active=sandbox -Djava.endorsed.dirs=${CATALINA_HOME}/endorsed  -Dcatalina.base=${CATALINA_HOME} -Dcatalina.home=${CATALINA_HOME} -Djava.io.tmpdir=${CATALINA_HOME}/temp  org.apache.hadoop.util.RunJar ${CATALINA_HOME}/bin/bootstrap.jar  org.apache.catalina.startup.Bootstrap start > ${CATALINA_HOME}/logs/kylin.log 2>&1 &
        echo "A new Kylin instance is started by $USER, stop it using \"kylin.sh stop\""
        echo "Please visit http://<your_hostname>:7070/kylin to play with the cubes! (Useranme: ADMIN, Password: KYLIN)"
        echo "You can check the log at ${CATALINA_HOME}/logs/kylin.log"

elif [ "$1" = "stop" ]; then
        # kill all tomcats started by current user
        ps -fu $USER | grep tomcat | grep -v "grep" | awk '{print $2}' | xargs kill
        echo "all tomcats started by $USER are terminated"

else
        echo "Usage: \"kylin.sh start\", or \"kylin.sh stop\""

fi


