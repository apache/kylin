#!/usr/bin/env bash

kubectl create configmap -n kylin hadoop-config --from-file=conf/core-site.xml \
                                                --from-file=conf/hdfs-site.xml \
                                                --from-file=conf/yarn-site.xml \
                                                --from-file=conf/mapred-site.xml \
                                                --dry-run -o yaml | kubectl apply -f -
kubectl create configmap -n kylin hive-config   --from-file=conf/hive-site.xml \
                                                --dry-run -o yaml | kubectl apply -f -
kubectl create configmap -n kylin hbase-config  --from-file=conf/hbase-site.xml \
                                                --dry-run -o yaml | kubectl apply -f -
kubectl create configmap -n kylin kylin-config  --from-file=conf/kylin.properties \
                                                --dry-run -o yaml | kubectl apply -f -
kubectl create configmap -n kylin krb5-config   --from-file=conf/krb5.conf \
                                                --dry-run -o yaml | kubectl apply -f -
kubectl create configmap -n kylin kylin-context --from-file=conf/applicationContext.xml \
                                                --dry-run -o yaml | kubectl apply -f -
