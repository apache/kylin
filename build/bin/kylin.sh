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

# set verbose=true to print more logs during start up




source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@
if [ "$verbose" = true ]; then
    shift
fi

mkdir -p ${KYLIN_HOME}/logs
mkdir -p ${KYLIN_HOME}/ext

source ${dir}/set-java-home.sh

function retrieveDependency() {
    #retrive $hive_dependency and $hbase_dependency
    if [[ -z $reload_dependency && `ls -1 ${dir}/cached-* 2>/dev/null | wc -l` -eq 5 ]]
    then
        echo "Using cached dependency..."
        source ${dir}/cached-hive-dependency.sh
        source ${dir}/cached-hbase-dependency.sh
        source ${dir}/cached-hadoop-conf-dir.sh
        source ${dir}/cached-kafka-dependency.sh
        source ${dir}/cached-spark-dependency.sh
    else
        source ${dir}/find-hive-dependency.sh
        source ${dir}/find-hbase-dependency.sh
        source ${dir}/find-hadoop-conf-dir.sh
        source ${dir}/find-kafka-dependency.sh
        source ${dir}/find-spark-dependency.sh
    fi

    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv.sh" ]; then
        echo "WARNING: ${dir}/setenv.sh is deprecated and ignored, please remove it and use ${KYLIN_HOME}/conf/setenv.sh instead"
        source ${dir}/setenv.sh
    fi
    
    if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv.sh
    fi

    export HBASE_CLASSPATH_PREFIX=${KYLIN_HOME}/conf:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH_PREFIX}
    export HBASE_CLASSPATH_PREFIX=${HBASE_CLASSPATH_PREFIX}:${hive_dependency}:${kafka_dependency}:${spark_dependency}

    verbose "HBASE_CLASSPATH: ${HBASE_CLASSPATH}"
}

function retrieveStartCommand() {
    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat $KYLIN_HOME/pid`
        if ps -p $PID > /dev/null
        then
          quit "Kylin is running, stop it first"
        fi
    fi

    source ${dir}/check-env.sh

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

    spring_profile=`bash ${dir}/get-properties.sh kylin.security.profile`
    if [ -z "$spring_profile" ]
    then
        quit 'Please set kylin.security.profile in kylin.properties, options are: testing, ldap, saml.'
    else
        verbose "kylin.security.profile is set to $spring_profile"
    fi

    retrieveDependency

    #additionally add tomcat libs to HBASE_CLASSPATH_PREFIX
    export HBASE_CLASSPATH_PREFIX=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*:${HBASE_CLASSPATH_PREFIX}

    kylin_rest_address=`hostname -f`":"`grep "<Connector port=" ${tomcat_root}/conf/server.xml |grep protocol=\"HTTP/1.1\" | cut -d '=' -f 2 | cut -d \" -f 2`
    kylin_rest_address_arr=(${kylin_rest_address//;/ })
    nc -z -w 5 ${kylin_rest_address_arr[0]} ${kylin_rest_address_arr[1]} 1>/dev/null 2>&1; nc_result=$?
    if [ $nc_result -eq 0 ]; then
        quit "Port ${kylin_rest_address} is not available, could not start Kylin."
    fi

    ${KYLIN_HOME}/bin/check-migration-acl.sh || { exit 1; }
    #debug if encounter NoClassDefError
    verbose "kylin classpath is: $(hbase classpath)"

    # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
    start_command="hbase ${KYLIN_EXTRA_START_OPTS} \
    -Djava.util.logging.config.file=${tomcat_root}/conf/logging.properties \
    -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
    -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-server-log4j.properties \
    -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true \
    -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
    -Djava.endorsed.dirs=${tomcat_root}/endorsed  \
    -Dcatalina.base=${tomcat_root} \
    -Dcatalina.home=${tomcat_root} \
    -Djava.io.tmpdir=${tomcat_root}/temp  \
    -Dkylin.hive.dependency=${hive_dependency} \
    -Dkylin.hbase.dependency=${hbase_dependency} \
    -Dkylin.kafka.dependency=${kafka_dependency} \
    -Dkylin.spark.dependency=${spark_dependency} \
    -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} \
    -Dkylin.server.host-address=${kylin_rest_address} \
    -Dspring.profiles.active=${spring_profile} \
    org.apache.hadoop.util.RunJar ${tomcat_root}/bin/bootstrap.jar  org.apache.catalina.startup.Bootstrap start"
}

if [ "$2" == "--reload-dependency" ]
then
    reload_dependency=1
fi

# start command
if [ "$1" == "start" ]
then
    retrieveStartCommand
    ${start_command} >> ${KYLIN_HOME}/logs/kylin.out 2>&1 & echo $! > ${KYLIN_HOME}/pid &

    echo ""
    echo "A new Kylin instance is started by $USER. To stop it, run 'kylin.sh stop'"
    echo "Check the log at ${KYLIN_HOME}/logs/kylin.log"
    echo "Web UI is at http://${kylin_rest_address_arr}/kylin"
    exit 0
    
# run command
elif [ "$1" == "run" ]
then
    retrieveStartCommand
    ${start_command}

# stop command
elif [ "$1" == "stop" ]
then
    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat $KYLIN_HOME/pid`
        WAIT_TIME=2
        LOOP_COUNTER=10
        if ps -p $PID > /dev/null
        then
            echo "Stopping Kylin: $PID"
            kill $PID

            for ((i=0; i<$LOOP_COUNTER; i++))
            do
                # wait to process stopped 
                sleep $WAIT_TIME
                if ps -p $PID > /dev/null ; then
                    echo "Stopping in progress. Will check after $WAIT_TIME secs again..."
                    continue;
                else
                    break;
                fi
            done

            # if process is still around, use kill -9
            if ps -p $PID > /dev/null
            then
                echo "Initial kill failed, getting serious now..."
                kill -9 $PID
                sleep 1 #give kill -9  sometime to "kill"
                if ps -p $PID > /dev/null
                then
                   quit "Warning, even kill -9 failed, giving up! Sorry..."
                fi
            fi

            # process is killed , remove pid file		
            rm -rf ${KYLIN_HOME}/pid
            echo "Kylin with pid ${PID} has been stopped."
            exit 0
        else
           quit "Kylin with pid ${PID} is not running"
        fi
    else
        quit "Kylin is not running"
    fi

# streaming command
elif [ "$1" == "streaming" ]
then
    if [ $# -lt 2 ]
    then
        echo "invalid input args $@"
        exit -1
    fi
    if [ "$2" == "start" ]
    then
        if [ -f "${KYLIN_HOME}/streaming_receiver_pid" ]
        then
            PID=`cat $KYLIN_HOME/streaming_receiver_pid`
            if ps -p $PID > /dev/null
            then
              echo "Kylin is running, stop it first"
            exit 1
            fi
        fi
        #retrive $hbase_dependency
        source ${dir}/find-hbase-dependency.sh
        #retrive $KYLIN_EXTRA_START_OPTS
        if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]
            then source ${KYLIN_HOME}/conf/setenv.sh
        fi

        mkdir -p ${KYLIN_HOME}/ext
        HBASE_CLASSPATH=`hbase classpath`
        #echo "hbase class path:"$HBASE_CLASSPATH
        STREAM_CLASSPATH=${KYLIN_HOME}/lib/streaming/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH}

        # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
        ${JAVA_HOME}/bin/java -cp $STREAM_CLASSPATH ${KYLIN_EXTRA_START_OPTS} \
        -Dlog4j.configuration=stream-receiver-log4j.properties\
        -DKYLIN_HOME=${KYLIN_HOME}\
        -Dkylin.hbase.dependency=${hbase_dependency} \
        org.apache.kylin.stream.server.StreamingReceiver $@ > ${KYLIN_HOME}/logs/streaming_receiver.out 2>&1 & echo $! > ${KYLIN_HOME}/streaming_receiver_pid &
        exit 0
    elif [ "$2" == "stop" ]
    then
        if [ ! -f "${KYLIN_HOME}/streaming_receiver_pid" ]
        then
            echo "streaming is not running, please check"
            exit 1
        fi
        PID=`cat ${KYLIN_HOME}/streaming_receiver_pid`
        if [ "$PID" = "" ]
        then
            echo "streaming is not running, please check"
            exit 1
        else
            echo "stopping streaming:$PID"
            WAIT_TIME=2
            LOOP_COUNTER=20
            if ps -p $PID > /dev/null
            then
                echo "Stopping Kylin: $PID"
                kill $PID

                for ((i=0; i<$LOOP_COUNTER; i++))
                do
                    # wait to process stopped
                    sleep $WAIT_TIME
                    if ps -p $PID > /dev/null ; then
                        echo "Stopping in progress. Will check after $WAIT_TIME secs again..."
                        continue;
                    else
                        break;
                    fi
                done

                # if process is still around, use kill -9
                if ps -p $PID > /dev/null
                then
                    echo "Initial kill failed, getting serious now..."
                    kill -9 $PID
                    sleep 1 #give kill -9  sometime to "kill"
                    if ps -p $PID > /dev/null
                    then
                       quit "Warning, even kill -9 failed, giving up! Sorry..."
                    fi
                fi

                # process is killed , remove pid file
                rm -rf ${KYLIN_HOME}/streaming_receiver_pid
                echo "Kylin with pid ${PID} has been stopped."
                exit 0
            else
               quit "Kylin with pid ${PID} is not running"
            fi
        fi
    elif [[ "$2" = org.apache.kylin.* ]]
    then
        source ${KYLIN_HOME}/conf/setenv.sh
        HBASE_CLASSPATH=`hbase classpath`
        #echo "hbase class path:"$HBASE_CLASSPATH
        STREAM_CLASSPATH=${KYLIN_HOME}/lib/streaming/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH}

        shift
        # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
        ${JAVA_HOME}/bin/java -cp $STREAM_CLASSPATH ${KYLIN_EXTRA_START_OPTS} \
        -Dlog4j.configuration=stream-receiver-log4j.properties\
        -DKYLIN_HOME=${KYLIN_HOME}\
        -Dkylin.hbase.dependency=${hbase_dependency} \
        "$@"
        exit 0
    fi

elif [ "$1" = "version" ]
then
    retrieveDependency
    exec hbase -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties org.apache.kylin.common.KylinVersion
    exit 0

elif [ "$1" = "diag" ]
then
    echo "'kylin.sh diag' no longer supported, use diag.sh instead"
    exit 0

# tool command
elif [[ "$1" = org.apache.kylin.* ]]
then
    retrieveDependency

    #retrive $KYLIN_EXTRA_START_OPTS from a separate file called setenv-tool.sh
    unset KYLIN_EXTRA_START_OPTS # unset the global server setenv config first
    if [ -f "${dir}/setenv-tool.sh" ]; then
        echo "WARNING: ${dir}/setenv-tool.sh is deprecated and ignored, please remove it and use ${KYLIN_HOME}/conf/setenv-tool.sh instead"
        source ${dir}/setenv-tool.sh
    fi

    if [ -f "${KYLIN_HOME}/conf/setenv-tool.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv-tool.sh
    fi
    hbase_pre_original=${HBASE_CLASSPATH_PREFIX}
    export HBASE_CLASSPATH_PREFIX=${KYLIN_HOME}/tool/*:${HBASE_CLASSPATH_PREFIX}
    exec hbase ${KYLIN_EXTRA_START_OPTS} -Dkylin.hive.dependency=${hive_dependency} -Dkylin.hbase.dependency=${hbase_dependency} -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties "$@"
    export HBASE_CLASSPATH_PREFIX=${hbase_pre_original}
else
    quit "Usage: 'kylin.sh [-v] start' or 'kylin.sh [-v] stop'"
fi
