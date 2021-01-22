---
layout: docs
title:  "Deploy Kylin on Kubernetes"
categories: install
permalink: /docs/install/kylin_on_kubernetes.html
since: v3.0.2
---

Kubernetes is a portable, extensible, open-source platform for managing containerized workloads and services, it facilitates both declarative configuration and automation. It has a large, rapidly growing ecosystem. Kubernetes services, support and tools are widely available.

Apache Kylin is an open source, distributed analytical data warehouse for big data. Deploy Kylin on Kubernetes cluster will reduce cost of maintenance and extension.

## Directory
Visit and download https://github.com/apache/kylin/tree/master/kubernetes and you will find three directory:

- **config** 
 Please update your configuration file here.
- **template** 
 This directory provides two deployment templates, one for quick-start purpose, another for production/distributed deployment.
    - The quick-start template is for one node deployment with an ALL kylin instance.
    - The production template is for multi-nodes deployment with a few job/query kylin instances. Moreover, some other services like memcached and filebeat(check doc at [ELK stack](https://www.elastic.co/what-is/elk-stack)) will help to satisfy log collection/query cache/session sharing demand.
- **docker** 
 Docker image is the pre-requirement of Kylin on Kubernetes, please check this directory if you need to build it yourself. For CDH5.x user, you may consider using a provided image on DockerHub.
 
---
 
## Pre-requirements
 
1. A hadoop cluster.
2. A K8s cluster, with sufficient system resources.
3. **kylin-client** image.
4. An Elasticsearch cluster(maybe optional).

## How to build docker image

### Hadoop-client image

What is a hadoop-client docker image and why do we need this?

As we all know, the node you want to deploy Kylin should contain Hadoop dependencies(jars and configuration files), these dependencies let you have access to Hadoop Services, such as HDFS, HBase, Hive, which are needed by Apache Kylin. Unfortunately, each Hadoop distribution(CHD or HDP etc.) has its own specific jars. So, we can build specific images for specific Hadoop distributions, which will make image management task easier. This will have the following two benefits:

- Someone who has more knowledge on Hadoop can do this work, and let kylin users build their Kylin image base on provided Hadoop-Client image.
- Upgrade Kylin will be much easier.

Build Step
- Prepare and modify Dockerfile(If you are using other hadoop distribution, please consider build an image yourself). 
- Place Spark binary(such as `spark-2.3.2-bin-hadoop2.7.tgz`) into dir `provided-binary`.
- Run `build-image.sh` to build the image.

### Kylin-client image
 
What is a kylin-client docker image? 

**kylin-client** is a docker image which based on **hadoop-client**, it will provide the flexibility of upgrade of Apache Kylin.

Build Step

- Place Kylin binary(such as `apache-kylin-3.0.1-bin-cdh57.tar.gz`) and uncompress it into current dir.
- Modify `Dockerfile` , change the value of `KYLIN_VERSION` and the name of base image(hadoop-client).
- Run `build-image.sh` to build image.

----

## How to deploy kylin on kubernetes

Here let's take a look at how to deploy a kylin cluster which connects to CDH 5.7.

1 `kubenetes/template/production/example/deployment` is the working directory.

2 Update hadoop configuration files (`kubenetes/template/production/example/config/hadoop`) and filebeat's configuration file.

3 Create statefulset and service for memcached.

- Apply kubernetes objects.

```
$ kubectl apply -f memcached/
service/cache-svc created
statefulset.apps/kylin-memcached created
```

- Check the hostname of cache service.

``` 
$ kubectl run -it--image=busybox:1.28.4--rm--restart=Never sh -n test-dns
If you don't see a command prompt, try pressing enter.
/ # nslookup cache-svc.kylin-example.svc.cluster.local
Server: 10.96.0.10
Address 1: 10.96.0.10 kube-dns.kube-system.svc.cluster.local
Name:   cache-svc.kylin-example.svc.cluster.local
Address 1: 192.168.11.44 kylin-memcached-0.cache-svc.kylin-example.svc.cluster.local
/ #
```

4 Create statefulset and service for Apache Kylin.
- Modify memcached configuration.

``` 
// modify memcached hostname(session sharing)
// memcachedNodes="n1:kylin-memcached-0.cache-svc.kylin-example.svc.cluster.local:11211"
$ vim ../config/tomcat/context.xml
// modify memcached hostname(query cache)
// kylin.cache.memcached.hosts=kylin-memcached-0.cache-svc.kylin-example.svc.cluster.local:11211
$ vim ../config/kylin-job/kylin.properties
$ vim ../config/kylin-query/kylin.properties
```

- Create the configMap

``` 
$ kubectl create configmap -n kylin-example hadoop-config \
--from-file=../config/hadoop/core-site.xml \
--from-file=../config/hadoop/hdfs-site.xml \
--from-file=../config/hadoop/yarn-site.xml \
--from-file=../config/hadoop/mapred-site.xml \
--dry-run -o yaml | kubectl apply -f -
$ kubectl create configmap -n kylin-example hive-config \
--from-file=../config/hadoop/hive-site.xml \
--dry-run -o yaml | kubectl apply -f -
$ kubectl create configmap -n kylin-example hbase-config \
--from-file=../config/hadoop/hbase-site.xml \
--dry-run -o yaml | kubectl apply -f -
$ kubectl create configmap -n kylin-example kylin-more-config \
--from-file=../config/kylin-more/applicationContext.xml \
--from-file=../config/kylin-more/ehcache.xml \
--from-file=../config/kylin-more/ehcache-test.xml \
--from-file=../config/kylin-more/kylinMetrics.xml \
--from-file=../config/kylin-more/kylinSecurity.xml \
--dry-run -o yaml | kubectl apply -f -
$ kubectl create configmap -n kylin-example kylin-job-config  \
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
$ kubectl create configmap -n kylin-example kylin-query-config  \
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
$ kubectl create configmap -n kylin-example filebeat-config  \
--from-file=../config/filebeat/filebeat.yml \
--dry-run -o yaml | kubectl apply -f -
$ kubectl create configmap -n kylin-example tomcat-config  \
--from-file=../config/tomcat/server.xml \
--from-file=../config/tomcat/context.xml \
--dry-run -o yaml | kubectl apply -f -
```

- Deploy Kylin's Job server

```
$ kubectl apply -f kylin-job/
service/kylin-job-svc created
statefulset.apps/kylin-job created
```
- Deploy Kylin's Query server
``` 
$ kubectl apply -f kylin-query/
service/kylin-query-svc created
statefulset.apps/kylin-query created
```

5 Kylin Service

- Visit Web UI
  - http://${HOSTNAME}:30012/kylin for Query Server

6 Clean up


``` 
$ kubectl delete -f memcached/
$ kubectl delete -f kylin-query/
$ kubectl delete -f kylin-job/
```

## Troubleshooting
- Check logs of specific pod
```
//  Output of : sh kylin.sh start
$ kubectl logs kylin-job-0 kylin -n kylin-example
$ kubectl logs -f kylin-job-0 kylin -n kylin-example
```
 
- Attach to a specific pod, say "kylin-job-0".   
``` 
$ kubectl exec -it  kylin-job-0  -n kylin-example-- bash
```   

- Check failure reasons of specific pod
``` 
$ kubectl get pod kylin-job-0  -n kylin-example -o yaml
```

- If you don't have an Elasticsearch cluster or not interested in log collection, please remove filebeat container in both kylin-query-stateful.yaml and kylin-job-stateful.yaml.

- If you want to check the details or want to have a discussion, please read or comment on [KYLIN-4447 Kylin on kubernetes in production env](https://issues.apache.org/jira/browse/KYLIN-4447) .

- Find the provided docker image at: DockerHub: : [apachekylin/kylin-client](https://hub.docker.com/r/apachekylin/kylin-client)