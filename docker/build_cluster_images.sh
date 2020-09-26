#!/bin/bash

ARGS=`getopt -o h:i:b:c:a:l:k:f --long hadoop_version:,hive_version:,hbase_version:,cluster_mode:,hbase:,ldap:,kerberos:,kafka -n 'parameter.bash' -- "$@"`

if [ $? != 0 ]; then
    echo "Terminating..."
    exit 1
fi

eval set -- "${ARGS}"

HADOOP_VERSION="2.8.5"
HIVE_VERSION="1.2.2"
HBASE_VERSION="1.1.2"

# write write-read
CLUSTER_MODE="write"
# yes,no
HBASE="yes"
# yes,no
LDAP="no"
# yes,no
KERBEROS="no"
#
KAFKA="no"

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
         --hbase)
            HBASE=$2;
            shift 2;
            ;;
        --ldap)
            LDAP=$2;
            shift 2;
            ;;
        --kerberos)
            KERBEROS=$2;
            shift 2;
            ;;
        --kafka)
            KAFKA=$2;
            shift 2;
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
echo "........hbase: "${HBASE}
echo "........ldap: "${LDAP}
echo "........kerberos: "${KERBEROS}


#docker build -t apachekylin/kylin-metastore:mysql_5.6.49 ./kylin/metastore-db
#
docker build -t apachekylin/kylin-hadoop-base:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/base
docker build -t apachekylin/kylin-hadoop-namenode:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/namenode
docker build -t apachekylin/kylin-hadoop-datanode:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/datanode
docker build -t apachekylin/kylin-hadoop-resourcemanager:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/resourcemanager
docker build -t apachekylin/kylin-hadoop-nodemanager:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/nodemanager
docker build -t apachekylin/kylin-hadoop-historyserver:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/historyserver

docker build -t apachekylin/kylin-hive:hive_${HIVE_VERSION}_hadoop_${HADOOP_VERSION} \
--build-arg HIVE_VERSION=${HIVE_VERSION} \
--build-arg HADOOP_VERSION=${HADOOP_VERSION} \
./dockerfile/cluster/hive

export HADOOP_NAMENODE_IMAGETAG=apachekylin/kylin-hadoop-base:hadoop_${HADOOP_VERSION}
export HADOOP_DATANODE_IMAGETAG=apachekylin/kylin-hadoop-datanode:hadoop_${HADOOP_VERSION}
export HADOOP_NAMENODE_IMAGETAG=apachekylin/kylin-hadoop-namenode:hadoop_${HADOOP_VERSION}
export HADOOP_RESOURCEMANAGER_IMAGETAG=apachekylin/kylin-hadoop-resourcemanager:hadoop_${HADOOP_VERSION}
export HADOOP_NODEMANAGER_IMAGETAG=apachekylin/kylin-hadoop-nodemanager:hadoop_${HADOOP_VERSION}
export HADOOP_HISTORYSERVER_IMAGETAG=apachekylin/kylin-hadoop-historyserver:hadoop_${HADOOP_VERSION}
export HIVE_IMAGETAG=apachekylin/kylin-hive:hive_${HIVE_VERSION}_hadoop_${HADOOP_VERSION}


if [ $HBASE == "yes" ]; then
  docker build -t apachekylin/kylin-hbase-base:hbase_${HBASE_VERSION} --build-arg HBASE_VERSION=${HBASE_VERSION} ./dockerfile/cluster/hbase
  docker build -t apachekylin/kylin-hbase-master:hbase_${HBASE_VERSION} --build-arg HBASE_VERSION=${HBASE_VERSION} ./dockerfile/cluster/hmaster
  docker build -t apachekylin/kylin-hbase-regionserver:hbase_${HBASE_VERSION} --build-arg HBASE_VERSION=${HBASE_VERSION} ./dockerfile/cluster/hregionserver

  export HBASE_MASTER_IMAGETAG=apachekylin/kylin-hbase-base:hbase_${HBASE_VERSION}
  export HBASE_MASTER_IMAGETAG=apachekylin/kylin-hbase-master:hbase_${HBASE_VERSION}
  export HBASE_REGIONSERVER_IMAGETAG=apachekylin/kylin-hbase-regionserver:hbase_${HBASE_VERSION}
fi

if [ $KERBEROS == "yes" ]; then
  docker build -t apachekylin/kylin-kerberos:latest ./dockerfile/cluster/kerberos
  export KERBEROS_IMAGE=apachekylin/kylin-kerberos:latest
fi

if [ $LDAP == "yes" ]; then
  docker pull osixia/openldap:1.3.0
  export LDAP_IMAGE=osixia/openldap:1.3.0
fi

if [ $KAFKA == "yes" ]; then
  docker pull bitnami/kafka:2.0.0
  export KAFKA_IMAGE=bitnami/kafka:2.0.0
fi

docker build -t apachekylin/kylin-client:hadoop_${HADOOP_VERSION}_hive_${HIVE_VERSION}_hbase_${HBASE_VERSION} \
--build-arg HIVE_VERSION=${HIVE_VERSION} \
--build-arg HADOOP_VERSION=${HADOOP_VERSION} \
--build-arg HBASE_VERSION=${HBASE_VERSION} \
./dockerfile/cluster/client

export CLIENT_IMAGETAG=apachekylin/kylin-client:hadoop_${HADOOP_VERSION}_hive_${HIVE_VERSION}_hbase_${HBASE_VERSION}


