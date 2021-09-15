---
layout: docs31
title:  "用 Docker 运行 Kylin"
categories: install
permalink: /cn/docs31/install/kylin_docker.html
since: v3.0.0
---

为了让用户方便的试用 Kylin，以及方便开发者在修改了源码后进行验证及调试。我们提供了 Kylin 的 docker 镜像。该镜像中，Kylin 依赖的各个服务均已正确的安装及部署，包括：

- JDK 1.8
- Hadoop 2.7.0
- Hive 1.2.1
- Hbase 1.1.2 (with Zookeeper)
- Spark 2.3.1
- Kafka 1.1.1
- MySQL 5.1.73

## 快速试用 Kylin

我们已将面向用户的 Kylin 镜像上传至 docker 仓库，用户无需在本地构建镜像，直接执行以下命令从 docker 仓库 pull 镜像：

{% highlight Groff markup %}
docker pull apachekylin/apache-kylin-standalone:3.1.0
{% endhighlight %}

pull 成功后，执行以下命令启动容器：

{% highlight Groff markup %}
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 16010:16010 \
apachekylin/apache-kylin-standalone:3.1.0
{% endhighlight %}

在容器启动时，会自动启动以下服务：

- NameNode, DataNode
- ResourceManager, NodeManager
- HBase
- Kafka
- Kylin

并自动运行 `$KYLIN_HOME/bin/sample.sh ` 及在 Kafka 中创建 "kylin_streaming_topic" topic 并持续向该 topic 中发送数据。这是为了让用户启动容器后，就能体验以批和流的方式的方式构建 Cube 并进行查询。

容器启动后，我们可以通过 "docker exec -it \<container_id\> bash" 命令进入容器内。当然，由于我们已经将容器内指定端口映射到本机端口，我们可以直接在本机浏览器中打开各个服务的页面，如：

- Kylin 页面：[http://127.0.0.1:7070/kylin/login](http://127.0.0.1:7070/kylin/login)
- HDFS NameNode 页面：[http://127.0.0.1:50070](http://127.0.0.1:50070/)
- YARN ResourceManager 页面：[http://127.0.0.1:8088](http://127.0.0.1:8088/)
- HBase 页面：[http://127.0.0.1:16010](http://127.0.0.1:16010/)

## 容器资源建议

为了让 Kylin 能够顺畅的构建 Cube，我们为 Yarn NodeManager 配置的内存资源为 6G，加上各服务占用的内存，请保证容器的内存不少于 8G，以免因为内存不足导致出错。

为容器设置资源方法请参考：

- Mac 用户：<https://docs.docker.com/docker-for-mac/#advanced>
- Linux 用户：<https://docs.docker.com/config/containers/resource_constraints/#memory>

---


关于如何定制修改 Docker image，请参阅 Git 代码库的 [kylin/docker](https://github.com/apache/kylin/tree/master/docker/)