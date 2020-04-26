echo """
Hello, welcome to deploy Kylin on Kubernetes.
BTW, this is a quick start template for demo usage.
"""

## create namespace
kubectl create namespace kylin-prod

## Create configmap
kubectl create configmap -n kylin-prod hadoop-config \
    --from-file=../../config/production/hadoop/core-site.xml \
    --from-file=../../config/production/hadoop/hdfs-site.xml \
    --from-file=../../config/production/hadoop/yarn-site.xml \
    --from-file=../../config/production/hadoop/mapred-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod hive-config \
    --from-file=../../config/production/hadoop/hive-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod hbase-config \
    --from-file=../../config/production/hadoop/hbase-site.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod kylin-more-config \
    --from-file=../../config/production/kylin-more/applicationContext.xml \
    --from-file=../../config/production/kylin-more/ehcache.xml \
    --from-file=../../config/production/kylin-more/ehcache-test.xml \
    --from-file=../../config/production/kylin-more/kylinMetrics.xml \
    --from-file=../../config/production/kylin-more/kylinSecurity.xml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod kylin-all-config  \
    --from-file=../../config/production/kylin-all/kylin-kafka-consumer.xml \
    --from-file=../../config/production/kylin-all/kylin_hive_conf.xml \
    --from-file=../../config/production/kylin-all/kylin_job_conf.xml \
    --from-file=../../config/production/kylin-all/kylin_job_conf_inmem.xml \
    --from-file=../../config/production/kylin-all/kylin-server-log4j.properties \
    --from-file=../../config/production/kylin-all/kylin-spark-log4j.properties \
    --from-file=../../config/production/kylin-all/kylin-tools-log4j.properties \
    --from-file=../../config/production/kylin-all/kylin.properties \
    --from-file=../../config/production/kylin-all/setenv.sh \
    --from-file=../../config/production/kylin-all/setenv-tool.sh \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod kylin-job-config  \
    --from-file=../../config/production/kylin-job/kylin-kafka-consumer.xml \
    --from-file=../../config/production/kylin-job/kylin_hive_conf.xml \
    --from-file=../../config/production/kylin-job/kylin_job_conf.xml \
    --from-file=../../config/production/kylin-job/kylin_job_conf_inmem.xml \
    --from-file=../../config/production/kylin-job/kylin-server-log4j.properties \
    --from-file=../../config/production/kylin-job/kylin-spark-log4j.properties \
    --from-file=../../config/production/kylin-job/kylin-tools-log4j.properties \
    --from-file=../../config/production/kylin-job/kylin.properties \
    --from-file=../../config/production/kylin-job/setenv.sh \
    --from-file=../../config/production/kylin-job/setenv-tool.sh \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod kylin-query-config  \
    --from-file=../../config/production/kylin-query/kylin-kafka-consumer.xml \
    --from-file=../../config/production/kylin-query/kylin_hive_conf.xml \
    --from-file=../../config/production/kylin-query/kylin_job_conf.xml \
    --from-file=../../config/production/kylin-query/kylin_job_conf_inmem.xml \
    --from-file=../../config/production/kylin-query/kylin-server-log4j.properties \
    --from-file=../../config/production/kylin-query/kylin-spark-log4j.properties \
    --from-file=../../config/production/kylin-query/kylin-tools-log4j.properties \
    --from-file=../../config/production/kylin-query/kylin.properties \
    --from-file=../../config/production/kylin-query/setenv.sh \
    --from-file=../../config/production/kylin-query/setenv-tool.sh \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod kylin-receiver-config  \
    --from-file=../../config/production/streaming-receiver/kylin.properties \
    --from-file=../../config/production/streaming-receiver/setenv.sh \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod filebeat-config  \
    --from-file=../../config/production/filebeat/filebeat.yml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-prod tomcat-config  \
    --from-file=../../config/production/tomcat/server.xml \
    --from-file=../../config/production/tomcat/context.xml \
    --dry-run -o yaml | kubectl apply -f -

### Prepare memcached service

kubectl create -f deployment/memcached/memcached-service.yaml
kubectl create -f deployment/memcached/memcached-statefulset.yaml


### Prepare kylin service


## Create headless serivce
kubectl create -f deployment/kylin/kylin-service.yaml

## Create statefulset
kubectl create -f deployment/kylin/kylin-all-statefulset.yaml
kubectl create -f deployment/kylin/kylin-job-statefulset.yaml

#kubectl create -f deployment/kylin/kylin-query-statefulset.yaml
#kubectl create -f deployment/kylin/kylin-receiver-statefulset.yaml

kubectl delete -f deployment/kylin/kylin-all-statefulset.yaml
kubectl delete -f deployment/kylin/kylin-job-statefulset.yaml


