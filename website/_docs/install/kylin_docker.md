---
layout: docs
title:  "Run Kylin with Docker"
categories: install
permalink: /docs/install/kylin_docker.html
since: v3.0.0
---

In order to allow users to easily try Kylin, and to facilitate developers to verify and debug after modifying the source code. We provide Kylin's docker image. In this image, each service that Kylin relies on is properly installed and deployed, including:

- Jdk 1.8
- Hadoop 2.7.0
- Hive 1.2.1
- Hbase 1.1.2 (with Zookeeper)
- Spark 2.3.1
- Kafka 1.1.1
- MySQL 5.1.73

## Quickly try Kylin

We have pushed the Kylin image for the user to the docker hub. Users do not need to build the image locally, just execute the following command to pull the image from the docker hub: 

{% highlight Groff markup %}
docker pull apachekylin/apache-kylin-standalone:3.1.0
{% endhighlight %}

After the pull is successful, execute the following command to start the container: 

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

The following services are automatically started when the container starts: 

- NameNode, DataNode
- ResourceManager, NodeManager
- HBase
- Kafka
- Kylin

and run automatically `$KYLIN_HOME/bin/sample.sh `, create a "kylin_streaming_topic" topic in Kafka and continue to send data to this topic. This is to let the users start the container and then experience the batch and streaming way to build the cube and query.

After the container is started, we can enter the container through the `docker exec -it <container_id> bash` command. Of course, since we have mapped the specified port in the container to the local port, we can open the pages of each service directly in the native browser, such as: 

- Kylin Web UI: [http://127.0.0.1:7070/kylin/login](http://127.0.0.1:7070/kylin/login)
- Hdfs NameNode Web UI: [http://127.0.0.1:50070](http://127.0.0.1:50070/)
- Yarn ResourceManager Web UI: [http://127.0.0.1:8088](http://127.0.0.1:8088/)
- HBase Web UI: [http://127.0.0.1:16010](http://127.0.0.1:16010/)

## Container resource recommendation

In order to allow Kylin to build the cube smoothly, the memory resource we configured for Yarn NodeManager is 6G, plus the memory occupied by each service, please ensure that the memory of the container is not less than 8G, so as to avoid errors due to insufficient memory.

For the resource setting method for the container, please refer to:

- Mac user: <https://docs.docker.com/docker-for-mac/#advanced>
- Linux user: <https://docs.docker.com/config/containers/resource_constraints/#memory>

---

For how to customize the image, please check the github page [kylin/docker](https://github.com/apache/kylin/tree/master/docker/).