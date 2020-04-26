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

## create namespace
kubectl create namespace kylin-quickstart

## Create configmap
kubectl create configmap -n kylin-quickstart hadoop-config \
    --from-file=../../config/quickstart/hadoop/core-site.xml \
    --from-file=../../config/quickstart/hadoop/hdfs-site.xml \
    --from-file=../../config/quickstart/hadoop/yarn-site.xml \
    --from-file=../../config/quickstart/hadoop/mapred-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-quickstart hive-config \
    --from-file=../../config/quickstart/hadoop/hive-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-quickstart hbase-config \
    --from-file=../../config/quickstart/hadoop/hbase-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-quickstart kylin-config  \
    --from-file=../../config/quickstart/kylin/kylin-kafka-consumer.xml \
    --from-file=../../config/quickstart/kylin/kylin_hive_conf.xml \
    --from-file=../../config/quickstart/kylin/kylin_job_conf.xml \
    --from-file=../../config/quickstart/kylin/kylin_job_conf_inmem.xml \
    --from-file=../../config/quickstart/kylin/kylin-server-log4j.properties \
    --from-file=../../config/quickstart/kylin/kylin-spark-log4j.properties \
    --from-file=../../config/quickstart/kylin/kylin-tools-log4j.properties \
    --from-file=../../config/quickstart/kylin/kylin.properties \
    --from-file=../../config/quickstart/kylin/setenv.sh \
    --from-file=../../config/quickstart/kylin/setenv-tool.sh \
    --dry-run -o yaml | kubectl apply -f -

## Create kylin serivce
kubectl apply -f deployment/kylin/