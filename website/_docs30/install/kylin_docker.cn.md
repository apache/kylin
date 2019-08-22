---
layout: docs30
title:  "用 Docker 运行 Kylin"
categories: install
permalink: /cn/docs30/install/kylin_docker.html
since: v3.0.0-alpha2
---

为了让用户方便的试用 Kylin，以及方便开发者在修改了源码后进行验证及调试。我们提供了 Kylin 的 docker 镜像。该镜像中，Kylin 依赖的各个服务均已正确的安装及部署，包括：

- Jdk 1.8
- Hadoop 2.7.0
- Hive 1.2.1
- Hbase 1.1.2
- Spark 2.3.1
- Zookeeper 3.4.6
- Kafka 1.1.1
- Mysql
- Maven 3.6.1

## 快速试用 Kylin

我们已将面向用户的 Kylin 镜像上传至 docker 仓库，用户无需在本地构建镜像，直接执行以下命令从 docker 仓库 pull 镜像：

{% highlight Groff markup %}
docker pull apachekylin/apache-kylin-standalone:3.0.0-alpha2
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
-p 60010:60010 \
apachekylin/apache-kylin-standalone:3.0.0-alpha2
{% endhighlight %}

在容器启动时，会自动启动以下服务：

- NameNode, DataNode
- ResourceManager, NodeManager
- HBase
- Kafka
- Kylin

并自动运行 `$KYLIN_HOME/bin/sample.sh ` 及在 Kafka 中创建 kylin_streaming_topic topic 并持续向该 topic 中发送数据。这是为了让用户启动容器后，就能体验以批和流的方式的方式构建 Cube 并进行查询。

容器启动后，我们可以通过 docker exec 命令进入容器内。当然，由于我们已经将容器内指定端口映射到本机端口，我们可以直接在本机浏览器中打开各个服务的页面，如：

- Kylin 页面：[http://127.0.0.1:7070/kylin/login](http://127.0.0.1:7070/kylin/login)
- Hdfs NameNode 页面：[http://127.0.0.1:50070](http://127.0.0.1:50070/)
- Yarn ResourceManager 页面：[http://127.0.0.1:8088](http://127.0.0.1:8088/)
- HBase 页面：[http://127.0.0.1:60010](http://127.0.0.1:60010/)

容器内，相关环境变量如下：

{% highlight Groff markup %}
JAVA_HOME=/home/admin/jdk1.8.0_141
HADOOP_HOME=/home/admin/hadoop-2.7.0
KAFKA_HOME=/home/admin/kafka_2.11-1.1.1
SPARK_HOME=/home/admin/spark-2.3.1-bin-hadoop2.6
HBASE_HOME=/home/admin/hbase-1.1.2
HIVE_HOME=/home/admin/apache-hive-1.2.1-bin
KYLIN_HOME=/home/admin/apache-kylin-3.0.0-alpha2-bin-hbase1x
{% endhighlight %}

## 构建镜像以验证源码修改

当开发者修改了源代码，想要对源代码进行打包、部署及验证时，也可以使用镜像。首先，我们进入源码根目录下的 docker 目录，并执行下面的脚本来构建镜像并将源码拷贝到镜像中：

{% highlight Groff markup %}
#!/usr/bin/env bash

echo "start build kylin image base on current source code"

rm -rf ./kylin
mkdir -p ./kylin

echo "start copy kylin source code"

for file in `ls ../../kylin/`
do
    if [ docker != $file ]
    then
        cp -r ../../kylin/$file ./kylin/
    fi
done

echo "finish copy kylin source code"

docker build -t apache-kylin-standalone .⏎
{% endhighlight %}

由于需要通过网络下载各种安装包并进行部署，整个构建过程可能会持续几十分钟，时间长短取决于网络情况。

当完成镜像构建后，执行以下命令启动容器：

{% highlight Groff markup %}
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 60010:60010 \
apache-kylin-standalone
{% endhighlight %}

当容器启动后，执行 docker exec 命令进入容器。源代码存放在容器的 `/home/admin/kylin_sourcecode` 目录，执行以下命令对源码进行打包：

{% highlight Groff markup %}
cd /home/admin/kylin_sourcecod
build/script/package.sh
{% endhighlight %}

打包完成后，会在 `/home/admin/kylin_sourcecode/dist` 目录下生成一个以 `.tar.gz` 结尾的安装包，如 `apache-kylin-3.0.0-alpha2-bin-hbase1x.tar.gz`。我们可以使用该安装包进行部署和启动 Kylin 服务，如：

{% highlight Groff markup %}
cp /home/admin/kylin_sourcecode/dist/apache-kylin-3.0.0-alpha2-bin-hbase1x.tar.gz /home/admin
tar -zxvf /home/admin/apache-kylin-3.0.0-alpha2-bin-hbase1x.tar.gz
/home/admin/apache-kylin-3.0.0-alpha2-bin-hbase1x/kylin.sh start
{% endhighlight %}

我们同样可以在本机的浏览器中打开 Hdfs、Yarn、HBase、Kylin 等服务的页面。

## 容器资源建议

为了让 Kylin 能够顺畅的构建 Cube，我们为 Yarn NodeManager 配置的内存资源为 6G，加上各服务占用的内存，请保证容器的内存不少于 8G，以免因为内存不足导致出错。

为容器设置资源方法请参考：

- Mac 用户：<https://docs.docker.com/docker-for-mac/#advanced>
- Linux 用户：<https://docs.docker.com/config/containers/resource_constraints/#memory>

---

旧版 docker image 请查看 github 项目 [kylin-docker](https://github.com/Kyligence/kylin-docker/).