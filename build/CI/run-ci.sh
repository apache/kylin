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

# This is a sample script for packaging & running system testing by docker container.

# 1. Packaging for Kylin binary
# 2. Deploy hadoop cluster
# 3. Delpoy kylin cluster
# 4. Run system testing
# 5. Clean up

INIT_HADOOP=${INIT_HADOOP:-1}

###########################################
###########################################
# 0. Prepare
AWAIT_SECOND=${AWAIT_SECOND:-240}
pwd

###########################################
###########################################
# 1. Package kylin
if [[ -z $binary_file ]]; then
  cd dev-support/build-release
  bash -x packaging.sh
  cd -
fi

binary_file=${binary_file:-apache-kylin-bin.tar.gz}

# 1.1 Prepare Kylin conf
cp $binary_file docker/docker-compose/others/kylin
cd docker/docker-compose/others/kylin
tar zxf apache-kylin-bin.tar.gz

mkdir kylin-all
mkdir kylin-query
mkdir kylin-job

cp -r apache-kylin-bin/* kylin-all
cat > kylin-all/conf/kylin.properties <<EOL
kylin.metadata.url=kylin_metadata@jdbc,url=jdbc:mysql://metastore-db:3306/metastore,username=kylin,password=kylin,maxActive=10,maxIdle=10
kylin.env.zookeeper-connect-string=write-zookeeper:2181
kylin.job.scheduler.default=100
kylin.engine.spark-conf.spark.shuffle.service.enabled=false
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl
kylin.engine.spark-conf.spark.executor.cores=4
EOL

#cp -r apache-kylin-bin/* kylin-query
#cat > kylin-query/conf/kylin.properties <<EOL
#kylin.job.scheduler.default=100
#kylin.server.self-discovery-enabled=true
#EOL
#
#cp -r apache-kylin-bin/* kylin-job
#cat > kylin-job/conf/kylin.properties <<EOL
#kylin.job.scheduler.default=100
#kylin.server.self-discovery-enabled=true
#EOL

cd -

###########################################
###########################################
# 2. Deploy Hadoop

if [ "$INIT_HADOOP" == "1" ];
then
    echo "Restart Hadoop cluster."
    cd docker

    bash stop_cluster.sh

    bash setup_hadoop_cluster.sh --cluster_mode write --hadoop_version 2.8.5 --hive_version 1.2.2 \
      --enable_hbase no --hbase_version 1.1.2  --enable_ldap nosh setup_cluster.sh \
      --cluster_mode write --hadoop_version 2.8.5 --hive_version 1.2.2 --enable_hbase yes \
      --hbase_version 1.1.2  --enable_ldap no
    cd ..
    sleep 100
else
    echo "Do NOT restart Hadoop cluster."
fi;

###########################################
###########################################
# 3. Deploy Kylin

echo "Restart Kylin cluster."

cd docker
bash setup_service.sh --cluster_mode write --hadoop_version 2.8.5 --hive_version 1.2.2 \
      --enable_hbase no --hbase_version 1.1.2  --enable_ldap nosh setup_cluster.sh \
      --cluster_mode write --hadoop_version 2.8.5 --hive_version 1.2.2 --enable_hbase yes \
      --hbase_version 1.1.2  --enable_ldap no
docker ps
cd ..

###########################################
###########################################
# 4. Run test

echo "Wait about 4 minutes ..."
sleep ${AWAIT_SECOND}

cd build/CI/kylin-system-testing
pip install -r requirements.txt
gauge run --tags 4.x
cd -
echo "Please check build/CI/kylin-system-testing/reports/html-report/index.html for reports."

###########################################
###########################################
# 5. Clean up

# TODO
