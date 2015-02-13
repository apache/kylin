#!/bin/sh

dir=$(dirname ${0})
source ${dir}/check-env.sh

tomcat_root=${dir}/../tomcat
export tomcat_root

#if [ ! -z "$KYLIN_LD_LIBRARY_PATH" ]
#then
#    echo "KYLIN_LD_LIBRARY_PATH is set to $KYLIN_LD_LIBRARY_PATH"
#else
#    exit 1
#fi

#The location of all hadoop/hbase configurations are difficult to get.
#Plus, some of the system properties are secretly set in hadoop/hbase shell command.
#For example, in hdp 2.2, there is a system property called hdp.version,
#which we cannot get until running hbase or hadoop shell command.
#
#To save all these troubles, we use hbase runjar to start tomcat.
#In this way we no longer need to explicitly configure hadoop/hbase related classpath for tomcat,
#hbase command will do all the dirty tasks for us:

#-Djava.library.path=${KYLIN_LD_LIBRARY_PATH} \


useSandbox=`cat ${KYLIN_HOME}/conf/kylin.properties | grep 'kylin.sandbox' | awk -F '=' '{print $2}'`
spring_profile="default"
if [ "$useSandbox" = "true" ]
    then spring_profile="sandbox"
fi
source ${dir}/find-hive-dependency.sh

export HBASE_CLASSPATH_PREFIX=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*:$HBASE_CLASSPATH_PREFIX
export HBASE_CLASSPATH=$hive_dependency:${HBASE_CLASSPATH}

hbase -Djava.util.logging.config.file=${tomcat_root}/conf/logging.properties \
-Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
-Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true \
-Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
-Djava.endorsed.dirs=${tomcat_root}/endorsed  \
-Dcatalina.base=${tomcat_root} \
-Dcatalina.home=${tomcat_root} \
-Djava.io.tmpdir=${tomcat_root}/temp  \
-Dkylin.hive.dependency=${hive_dependency} \
-Dspring.profiles.active=${spring_profile} \
org.apache.hadoop.util.RunJar ${tomcat_root}/bin/bootstrap.jar  org.apache.catalina.startup.Bootstrap start > ${tomcat_root}/logs/kylin.log 2>&1 & echo $! > ${KYLIN_HOME}/pid &
echo "A new Kylin instance is started by $USER, stop it using \"stop-kylin.sh\""
if [ "$useSandbox" = "true" ]
    then echo "Please visit http://<your_sandbox_ip>:7070/kylin to play with the cubes! (Useranme: ADMIN, Password: KYLIN)"
else
    echo "Please visit http://<ip>:7070/kylin"
fi
echo "You can check the log at ${tomcat_root}/logs/kylin.log"
