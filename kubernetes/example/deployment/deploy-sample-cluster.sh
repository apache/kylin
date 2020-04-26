
systemctl status docker-ce
systemctl status kube-apiserver
systemctl status kube-controller-manager
systemctl status kube-scheduler
systemctl status kubelet
systemctl status kube-proxy


## create namespace
kubectl create namespace kylin-example

## Create configmap
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

kubectl create configmap -n kylin-example kylin-all-config  \
    --from-file=../config/kylin-all/kylin-kafka-consumer.xml \
    --from-file=../config/kylin-all/kylin_hive_conf.xml \
    --from-file=../config/kylin-all/kylin_job_conf.xml \
    --from-file=../config/kylin-all/kylin_job_conf_inmem.xml \
    --from-file=../config/kylin-all/kylin-server-log4j.properties \
    --from-file=../config/kylin-all/kylin-spark-log4j.properties \
    --from-file=../config/kylin-all/kylin-tools-log4j.properties \
    --from-file=../config/kylin-all/kylin.properties \
    --from-file=../config/kylin-all/setenv.sh \
    --from-file=../config/kylin-all/setenv-tool.sh \
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

kubectl create configmap -n kylin-example kylin-receiver-config  \
    --from-file=../config/streaming-receiver/kylin.properties \
    --from-file=../config/streaming-receiver/setenv.sh \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example filebeat-config  \
    --from-file=../config/filebeat/filebeat.yml \
    --dry-run -o yaml | kubectl apply -f -

kubectl create configmap -n kylin-example tomcat-config  \
    --from-file=../config/tomcat/server.xml \
    --from-file=../config/tomcat/context.xml \
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


