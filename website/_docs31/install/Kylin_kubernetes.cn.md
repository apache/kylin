---
layout: docs31
title:  "在 Kubernetes 上部署 Kylin"
categories: install
permalink: /cn/docs31/install/kylin_on_kubernetes.html
since: v3.0.2
---

Kubernetes是一个易于移植，易于扩展的，用于管理容器化的 workload 和 service 的开源平台，通过声明式的配置和自动化，使得集群运维负担大大减少。

Apache Kylin 是一个开源的的分布式的，针对大数据场景下的高性能分析型数据仓库。在 Kubernetes 集群上部署 Kylin，可以减少 Kylin 集群维护和扩容的代价。

## 目录
下载获取 https://github.com/apache/kylin/tree/master/kubernetes, 得到以下文件:

- **config** 
 这里是存放 Hadoop 和 Kylin 配置文件的地方。
- **template** 
 这个目录提供了两个部署模板： 
    - Quick-start 模板为了单节点（Poc）目的准备的；只部署了一个 **all** 节点。
    - Production 模板是为多节点部署准备的；此外还集成了 Memcached 服务以满足分布式查询缓存需求，以及 filebeat([ELK stack](https://www.elastic.co/what-is/elk-stack)) 以满足日志收集和分析需求。
- **docker** 
 Docker 镜像是在 Kubernetes 上部署 Kylin 的前置条件，请参考此路径的说明，来构建所需镜像。CDH5.x 用户可以直接从 Dockerhub 获取预先构建好的镜像。
 
## 前置条件
 
1. 一个 Hadoop 集群
2. 一个 Kubernetes 集群
3. **kylin-client** 镜像
4. 一个 Elasticsearch 集群(可选的)

## 如何构建所需 docker 镜像

#### hadoop-client 镜像

我们都知道，部署 Kylin 的节点需要包含 Hadoop 依赖，以便 Kylin 通过客户端访问 Hadoop 集群的各个组件。不幸地，每一个 Hadoop 分发版本各自不同。所以我们可以为所需的 Hadoop 分发版本构建 Docker 镜像，会有以下好处：
- 可以让对特定 Hadoop 环境更加熟悉的人负责准备该镜像，让 Kylin 用户更加方便地基于 Hadoop-Client 镜像构建 Kylin 镜像。
- Kylin 的升级会更加方便。

构建步骤
- 准备或者修改 Dockerfile。 
- 获取 Spark 二进制包(例如 `spark-2.3.2-bin-hadoop2.7.tgz`) 到 路径`provided-binary`。
- 运行 `build-image.sh` 来构建镜像。

#### Kylin-client 镜像

**kylin-client** 是一个基于 **hadoop-client** 的镜像, 两者分离使得 Kylin 升级更加简单。

构建步骤
- 获取 Kylin 二进制包(例如 `apache-kylin-3.0.1-bin-cdh57.tar.gz`) 在当前目录解压。
- 修改 `Dockerfile` , 将 `KYLIN_VERSION` 和 base image(hadoop-client) 改为合适的值。
- 运行 `build-image.sh` 来构建镜像。

## 如何在 Kubernetes 部署 Kylin

在这里，我们以如何基于 CDH5.x 环境部署 Kylin 集群。

1 进入到 `kubenetes/template/production/example/deployment` 路径

2 替换 Hadoop 配置文件到 (`kubenetes/template/production/example/config/hadoop`) 并且修改 filebeat 配置文件。

3 部署 Memcached 服务
- Apply kubernetes objects.

```
$ kubectl apply -f memcached/
service/cache-svc created
statefulset.apps/kylin-memcached created
```

- 获取 Memcached 服务地址

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

4 部署 Kylin 服务
- 修改 Memcached 配置

``` 
## modify memcached hostname(session sharing)
## memcachedNodes="n1:kylin-memcached-0.cache-svc.kylin-example.svc.cluster.local:11211"
$ vim ../config/tomcat/context.xml
modify memcached hostname(query cache)
## kylin.cache.memcached.hosts=kylin-memcached-0.cache-svc.kylin-example.svc.cluster.local:11211
$ vim ../config/kylin-job/kylin.properties
$ vim ../config/kylin-query/kylin.properties
```

- 创建 ConfigMap

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

- 部署 Kylin 的 Job server

```
$ kubectl apply -f kylin-job/
service/kylin-job-svc created
statefulset.apps/kylin-job created
```

- 部署 Kylin 的 Query server

``` 
$ kubectl apply -f kylin-query/
service/kylin-query-svc created
statefulset.apps/kylin-query created
```

5 访问 Web UI
  - http://${HOSTNAME}:30012/kylin 对应 QueryServer

6 停止 Kylin 服务

``` 
$ kubectl delete -f memcached/
$ kubectl delete -f kylin-query/
$ kubectl delete -f kylin-job/
```

## 问题诊断
- 获取指定 Pod 的日志

```
##  Output of : sh kylin.sh start
$ kubectl logs kylin-job-0 kylin -n kylin-example
$ kubectl logs -f kylin-job-0 kylin -n kylin-example
```
 
- 访问指定 Pod

``` 
$ kubectl exec -it  {POD_NAME} -n kylin-example-- bash
```   

- 检查指定 Pod 启动失败原因

``` 
$ kubectl get pod {POD_NAME} -n kylin-example -o yaml
```

- 如果你没有 Elasticsearch 集群，或者有替代的日志收集方案, 请从 `kylin-query-stateful.yaml` 和 `kylin-job-stateful.yaml` 移除 **filebeat** 容器。

- JIRA issue: [KYLIN-4447 Kylin on kubernetes in production env](https://issues.apache.org/jira/browse/KYLIN-4447) 。

- 尝试在 DockerHub 上寻找预先构建好的 kylin-client image: [apachekylin/kylin-client](https://hub.docker.com/r/apachekylin/kylin-client). 