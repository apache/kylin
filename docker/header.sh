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

ARGS=`getopt -o h:i:b:c:a:l:k:f:p --long hadoop_version:,hive_version:,hbase_version:,cluster_mode:,enable_hbase:,enable_ldap:,enable_kerberos:,enable_kafka,help  -n 'parameter.bash' -- "$@"`

if [ $? != 0 ]; then
    echo "Terminating..."
    exit 1
fi

eval set -- "${ARGS}"

HADOOP_VERSION="2.8.5"
HIVE_VERSION="1.2.2"
HBASE_VERSION="1.1.2"
# write,write-read
CLUSTER_MODE="write"
# yes,no
ENABLE_HBASE="yes"
# yes,no
ENABLE_LDAP="no"
# yes,no
ENABLE_KERBEROS="no"
# yes,no
ENABLE_KAFKA="no"

while true;
do
    case "$1" in
        --hadoop_version)
            HADOOP_VERSION=$2;
            shift 2;
            ;;
        --hive_version)
            HIVE_VERSION=$2;
            shift 2;
            ;;
        --hbase_version)
            HBASE_VERSION=$2;
            shift 2;
            ;;
        --cluster_mode)
            CLUSTER_MODE=$2;
            shift 2;
            ;;
         --enable_hbase)
            ENABLE_HBASE=$2;
            shift 2;
            ;;
        --enable_ldap)
            ENABLE_LDAP=$2;
            shift 2;
            ;;
        --enable_kerberos)
            ENABLE_KERBEROS=$2;
            shift 2;
            ;;
        --enable_kafka)
            ENABLE_KAFKA=$2;
            shift 2;
            ;;
        --help)
echo << EOF "
----------------------menu------------------------
--hadoop_version  hadoop version,default is 2.8.5
--hive_version  hive version,default is 1.2.2
--hbase_version hbase version,default is 1.1.2
--cluster_mode  cluster mode, optional value : [write, write-read],default is write,
--enable_hbase  whether enable hbase server, optional value : [yes, no], default is yes
--enable_ldap whether enable ldap server, optional value : [yes, no], default is no
--enable_kerberos whether enable kerberos server, optional value : [yes, no], default is no
--enable_kafka whether enable kafka server, optional value : [yes, no], default is no"
EOF
            exit 0
            ;;
        --)
            break
            ;;
        *)
            echo "Internal error!"
            break
            ;;
    esac
done

for arg in $@
do
    echo "processing $arg"
done

echo "........hadoop version: "$HADOOP_VERSION
echo "........hive version: "$HIVE_VERSION
echo "........hbase version: "$HBASE_VERSION
echo "........cluster_mode: "${CLUSTER_MODE}
echo "........enable hbase: "${ENABLE_HBASE}
echo "........enable ldap: "${ENABLE_LDAP}
echo "........enable kerberos: "${ENABLE_KERBEROS}

export HBASE_VERSION=$HBASE_VERSION
export HADOOP_VERSION=$HADOOP_VERSION
export HIVE_VERSION=$HIVE_VERSION

export HADOOP_NAMENODE_IMAGETAG=apachekylin/kylin-ci-hadoop-base:hadoop_${HADOOP_VERSION}
export HADOOP_DATANODE_IMAGETAG=apachekylin/kylin-ci-hadoop-datanode:hadoop_${HADOOP_VERSION}
export HADOOP_NAMENODE_IMAGETAG=apachekylin/kylin-ci-hadoop-namenode:hadoop_${HADOOP_VERSION}
export HADOOP_RESOURCEMANAGER_IMAGETAG=apachekylin/kylin-ci-hadoop-resourcemanager:hadoop_${HADOOP_VERSION}
export HADOOP_NODEMANAGER_IMAGETAG=apachekylin/kylin-ci-hadoop-nodemanager:hadoop_${HADOOP_VERSION}
export HADOOP_HISTORYSERVER_IMAGETAG=apachekylin/kylin-ci-hadoop-historyserver:hadoop_${HADOOP_VERSION}
export HIVE_IMAGETAG=apachekylin/kylin-ci-hive:hive_${HIVE_VERSION}_hadoop_${HADOOP_VERSION}

export HBASE_MASTER_IMAGETAG=apachekylin/kylin-ci-hbase-base:hbase_${HBASE_VERSION}
export HBASE_MASTER_IMAGETAG=apachekylin/kylin-ci-hbase-master:hbase_${HBASE_VERSION}
export HBASE_REGIONSERVER_IMAGETAG=apachekylin/kylin-ci-hbase-regionserver:hbase_${HBASE_VERSION}

export KAFKA_IMAGE=bitnami/kafka:2.0.0
export LDAP_IMAGE=osixia/openldap:1.3.0
export CLIENT_IMAGETAG=apachekylin/kylin-ci-client:hadoop_${HADOOP_VERSION}_hive_${HIVE_VERSION}_hbase_${HBASE_VERSION}

if [[ $HADOOP_VERSION < "3" ]]; then
  export HADOOP_WEBHDFS_PORT=50070
  export HADOOP_DN_PORT=50075
elif [[ $HADOOP_VERSION > "3" ]]; then
  export HADOOP_WEBHDFS_PORT=9870
  export HADOOP_DN_PORT=9864
fi
