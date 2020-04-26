## create namespace
kubectl create namespace kylin-quickstart

## Prepare hadoop configuration


## Prepare kylin configuration


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

## Create headless serivce
kubectl delete -f deployment/kylin/kylin-service.yaml

## Create statefulset
kubectl create -f deployment/kylin/kylin-all-statefulset.yaml