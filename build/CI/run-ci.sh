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

# 1. Packaging for Kylin binary
# 2. Deploy hadoop cluster
# 3. Delpoy kylin cluster
# 4. Run system testing
# 5. Clean up

INIT_HADOOP=1

###########################################
###########################################
# 0. Prepare
export JAVA_HOME=/usr/local/java
export PATH=$JAVA_HOME/bin:$PATH
export PATH=/root/xiaoxiang.yu/INSTALL/anaconda/bin:$PATH
binary_file=/root/xiaoxiang.yu/BINARY/apache-kylin-3.1.2-SNAPSHOT-bin.tar.gz
source ~/.bashrc
pwd

###########################################
###########################################
# 1. Package kylin

#TODO
cd docker/docker-compose/others/kylin
cp $binary_file .
tar zxf apache-kylin-3.1.2-SNAPSHOT-bin.tar.gz

mkdir kylin-all
mkdir kylin-query
mkdir kylin-job

cp -r apache-kylin-3.1.2-SNAPSHOT-bin/* kylin-all
cat > kylin-all/conf/kylin.properties <<EOL
kylin.job.scheduler.default=100
kylin.server.self-discovery-enabled=true
EOL

cp -r apache-kylin-3.1.2-SNAPSHOT-bin/* kylin-query
cat > kylin-query/conf/kylin.properties <<EOL
kylin.job.scheduler.default=100
kylin.server.self-discovery-enabled=true
EOL

cp -r apache-kylin-3.1.2-SNAPSHOT-bin/* kylin-job
cat > kylin-job/conf/kylin.properties <<EOL
kylin.job.scheduler.default=100
kylin.server.self-discovery-enabled=true
EOL

cd -

###########################################
###########################################
# 2. Deploy Hadoop

if [ "$INIT_HADOOP" = "1" ];
then
    echo "Restart Hadoop cluster."
    cd docker

    bash stop_cluster.sh

    bash setup_cluster.sh --cluster_mode write --hadoop_version 2.8.5 --hive_version 1.2.2 \
      --enable_hbase yes --hbase_version 1.1.2  --enable_ldap nosh setup_cluster.sh \
      --cluster_mode write --hadoop_version 2.8.5 --hive_version 1.2.2 --enable_hbase yes \
      --hbase_version 1.1.2  --enable_ldap no
    cd ..
else
    echo "Do NOT restart Hadoop cluster."
fi;

docker ps

###########################################
###########################################
# 3. Deploy Kylin

# TODO

###########################################
###########################################
# 4. Run test

echo "Wait about 6 minutes ..."
sleep 360

cd build/CI/testing
pip install -r requirements.txt
gauge run --tags 3.x
cd ..

###########################################
###########################################
# 5. Clean up

# TODO
