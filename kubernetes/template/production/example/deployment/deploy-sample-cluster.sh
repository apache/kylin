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

## Step1. Create namespace
kubectl create namespace kylin-example

## Step2. Create configmap(please consider using secret in production env)
kubectl create configmap -n kylin-example hadoop-config \
    --from-file=../config/hadoop/core-site.xml \
    --from-file=../config/hadoop/hdfs-site.xml \
    --from-file=../config/hadoop/yarn-site.xml \
    --from-file=../config/hadoop/mapred-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example hive-config \
    --from-file=../config/hadoop/hive-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example hbase-config \
    --from-file=../config/hadoop/hbase-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example kylin-more-config \
    --from-file=../config/kylin-more/applicationContext.xml \
    --from-file=../config/kylin-more/ehcache.xml \
    --from-file=../config/kylin-more/ehcache-test.xml \
    --from-file=../config/kylin-more/kylinMetrics.xml \
    --from-file=../config/kylin-more/kylinSecurity.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example kylin-job-config  \
    --from-file=../config/kylin-job/kylin-kafka-consumer.xml \
    --from-file=../config/kylin-job/kylin_hive_conf.xml \
    --from-file=../config/kylin-job/kylin_job_conf.xml \
    --from-file=../config/kylin-job/kylin_job_conf_inmem.xml \
    --from-file=../config/kylin-job/kylin-server-log4j.properties \
    --from-file=../config/kylin-job/kylin-spark-log4j.properties \
    --from-file=../config/kylin-job/kylin-tools-log4j.properties \
    --from-file=../config/kylin-job/kylin.properties \
    --from-file=../config/kylin-job/setenv.sh \
    --from-file=../config/kylin-job/setenv-tool.sh \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example kylin-query-config  \
    --from-file=../config/kylin-query/kylin-kafka-consumer.xml \
    --from-file=../config/kylin-query/kylin_hive_conf.xml \
    --from-file=../config/kylin-query/kylin_job_conf.xml \
    --from-file=../config/kylin-query/kylin_job_conf_inmem.xml \
    --from-file=../config/kylin-query/kylin-server-log4j.properties \
    --from-file=../config/kylin-query/kylin-spark-log4j.properties \
    --from-file=../config/kylin-query/kylin-tools-log4j.properties \
    --from-file=../config/kylin-query/kylin.properties \
    --from-file=../config/kylin-query/setenv.sh \
    --from-file=../config/kylin-query/setenv-tool.sh \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example filebeat-config  \
    --from-file=../config/filebeat/filebeat.yml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example tomcat-config  \
    --from-file=../config/tomcat/server.xml \
    --from-file=../config/tomcat/context.xml \
    --dry-run -o yaml | kubectl apply -f -

## Step3. Deploy all service
kubectl apply -f memcached
kubectl apply -f kylin-job
kubectl apply -f kylin-query

## Step4. Check state
kubectl get pod -n kylin-example
kubectl get service -n kylin-example
kubectl get statefulset -n kylin-example

## Step5. Delete all(kylin + memcached) cluster
kubectl apply -f memcached
kubectl apply -f kylin-job
kubectl apply -f kylin-query