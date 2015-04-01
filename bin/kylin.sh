#!/bin/sh

dir=$(dirname ${0})
source ${dir}/check-env.sh

if [ $1 == "start" ]
then

    tomcat_root=${dir}/../tomcat
    export tomcat_root


    #The location of all hadoop/hbase configurations are difficult to get.
    #Plus, some of the system properties are secretly set in hadoop/hbase shell command.
    #For example, in hdp 2.2, there is a system property called hdp.version,
    #which we cannot get until running hbase or hadoop shell command.
    #
    #To save all these troubles, we use hbase runjar to start tomcat.
    #In this way we no longer need to explicitly configure hadoop/hbase related classpath for tomcat,
    #hbase command will do all the dirty tasks for us:



    useSandbox=`cat ${KYLIN_HOME}/conf/kylin.properties | grep 'kylin.sandbox' | awk -F '=' '{print $2}'`
    spring_profile="default"
    if [ "$useSandbox" = "true" ]
        then spring_profile="sandbox"
    fi

    #retrive $hive_dependency
    source ${dir}/find-hive-dependency.sh
    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv.sh" ]
        then source ${dir}/setenv.sh
    fi

    export HBASE_CLASSPATH_PREFIX=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*:$HBASE_CLASSPATH_PREFIX
    export HBASE_CLASSPATH=$hive_dependency:${HBASE_CLASSPATH}
    export JAVA_OPTS="-Xms2048M -Xmx2048M"

    hbase ${KYLIN_EXTRA_START_OPTS} \
    -Djava.util.logging.config.file=${tomcat_root}/conf/logging.properties \
    -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
    -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true \
    -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
    -Djava.endorsed.dirs=${tomcat_root}/endorsed  \
    -Dcatalina.base=${tomcat_root} \
    -Dcatalina.home=${tomcat_root} \
    -Djava.io.tmpdir=${tomcat_root}/temp  \
    -Dkylin.hive.dependency=${hive_dependency} \
    -Dspring.profiles.active=${spring_profile} \
    org.apache.hadoop.util.RunJar ${tomcat_root}/bin/bootstrap.jar  org.apache.catalina.startup.Bootstrap start > ${KYLIN_HOME}/logs/kylin.log 2>&1 & echo $! > ${KYLIN_HOME}/pid &
    echo "A new Kylin instance is started by $USER, stop it using \"kylin.sh stop\""
    if [ "$useSandbox" = "true" ]
        then echo "Please visit http://<your_sandbox_ip>:7070/kylin to play with the cubes! (Useranme: ADMIN, Password: KYLIN)"
    else
        echo "Please visit http://<ip>:7070/kylin"
    fi
    echo "You can check the log at ${KYLIN_HOME}/logs/kylin.log"
    exit 0
elif [ $1 == "stop" ]
then
    if [ ! -f "${KYLIN_HOME}/pid" ]
    then
        echo "kylin is not running, please check"
        exit 1
    fi
    pid=`cat ${KYLIN_HOME}/pid`
    if [ "$pid" = "" ]
    then
        echo "kylin is not running, please check"
        exit 1
    else
        echo "stopping kylin:$pid"
        kill $pid
    fi
    rm ${KYLIN_HOME}/pid
    exit 0
elif [ $1 == "streaming" ]
then
    if [ $# != 4 ]
    then
        echo 'invalid input args'
        exit -1
    fi
    if [ $2 == "start" ]
    then
        useSandbox=`cat ${KYLIN_HOME}/conf/kylin.properties | grep 'kylin.sandbox' | awk -F '=' '{print $2}'`
        spring_profile="default"
        if [ "$useSandbox" = "true" ]
            then spring_profile="sandbox"
        fi

        #retrive $hive_dependency
        source ${dir}/find-hive-dependency.sh
        #retrive $KYLIN_EXTRA_START_OPTS
        if [ -f "${dir}/setenv.sh" ]
            then source ${dir}/setenv.sh
        fi

        export HBASE_CLASSPATH=$hive_dependency:${HBASE_CLASSPATH}
        export JAVA_OPTS="-Xms2048M -Xmx2048M"

        hbase ${KYLIN_EXTRA_START_OPTS} \
        -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
        -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
        -Dkylin.hive.dependency=${hive_dependency} \
        -Dspring.profiles.active=${spring_profile} \
        org.apache.hadoop.util.RunJar ${KYLIN_HOME}/lib/kylin-job-*.jar org.apache.kylin.job.streaming.StreamingCLI start $3 $4 > ${KYLIN_HOME}/logs/streaming_$3_$4.log 2>&1 & echo $! > ${KYLIN_HOME}/$3_$4 &
        echo "streaming started $3 partition $4"
        exit 0
    elif [ $2 == "stop" ]
    then
        if [ ! -f "${KYLIN_HOME}/$3_$4" ]
        then
            echo "streaming is not running, please check"
            exit 1
        fi
        pid=`cat ${KYLIN_HOME}/$3_$4`
        if [ "$pid" = "" ]
        then
            echo "streaming is not running, please check"
            exit 1
        else
            echo "stopping streaming:$pid"
            kill $pid
        fi
        rm ${KYLIN_HOME}/$3_$4
        exit 0
    else
        echo
    fi
else
    echo "usage: kylin.sh start or kylin.sh stop"
    exit 1
fi
