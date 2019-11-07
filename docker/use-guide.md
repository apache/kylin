
In order to allow users to easily try Kylin, and to facilitate developers to verify and debug after modifying the source code. We provide Kylin's docker image. In this image, each service that Kylin relies on is properly installed and deployed, including:

- Jdk 1.8
- Hadoop 2.7.0
- Hive 1.2.1
- Hbase 1.1.2
- Spark 2.3.1
- Zookeeper 3.4.6
- Kafka 1.1.1
- Mysql
- Maven 3.6.1

## Quickly try Kylin

We have pushed the Kylin image for the user to the docker hub. Users do not need to build the image locally, just execute the following command to pull the image from the docker hub: 

```
docker pull apachekylin/apache-kylin-standalone:3.0.0-alpha2
```

After the pull is successful, execute the following command to start the container: 

```
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 60010:60010 \
apachekylin/apache-kylin-standalone:3.0.0-alpha2
```

The following services are automatically started when the container starts: 

- NameNode, DataNode
- ResourceManager, NodeManager
- HBase
- Kafka
- Kylin

and run automatically `$KYLIN_HOME/bin/sample.sh `, create a kylin_streaming_topic topic in Kafka and continue to send data to this topic. This is to let the users start the container and then experience the batch and streaming way to build the cube and query.

After the container is started, we can enter the container through the `docker exec` command. Of course, since we have mapped the specified port in the container to the local port, we can open the pages of each service directly in the native browser, such as: 

- Kylin Web UI: [http://127.0.0.1:7070/kylin/login](http://127.0.0.1:7070/kylin/login)
- Hdfs NameNode Web UI: [http://127.0.0.1:50070](http://127.0.0.1:50070/)
- Yarn ResourceManager Web UI: [http://127.0.0.1:8088](http://127.0.0.1:8088/)
- HBase Web UI: [http://127.0.0.1:60010](http://127.0.0.1:60010/)

In the container, the relevant environment variables are as follows: 

```
JAVA_HOME=/home/admin/jdk1.8.0_141
HADOOP_HOME=/home/admin/hadoop-2.7.0
KAFKA_HOME=/home/admin/kafka_2.11-1.1.1
SPARK_HOME=/home/admin/spark-2.3.1-bin-hadoop2.6
HBASE_HOME=/home/admin/hbase-1.1.2
HIVE_HOME=/home/admin/apache-hive-1.2.1-bin
KYLIN_HOME=/home/admin/apache-kylin-3.0.0-alpha2-bin-hbase1x
```

## Build image to verify source code modifications

The docker image can also be used when developers have modified the source code and want to package, deploy, and verify the source code. First, we go to the docker directory under the root directory of the source and execute the script below to build the image and copy the source into the image.: 

```
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
```

Due to need to download and deploy various binary packages over the network, the entire build process can last for tens of minutes, depending on the network.

When the image build is complete, execute the following command to start the container: 

```
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 60010:60010 \
apache-kylin-standalone
```

When the container starts, execute the docker exec command to enter the container. The source code is stored in the container dir `/home/admin/kylin_sourcecode`, execute the following command to package the source code: 

```
cd /home/admin/kylin_sourcecod
build/script/package.sh
```

After the package is complete, an binary package ending in `.tar.gz` will be generated in the `/home/admin/kylin_sourcecode/dist` directory, such as `apache-kylin-3.0.0-alpha2-bin-hbase1x.tar.gz`. We can use this  binary package to deploy and launch Kylin services such as:

```
cp /home/admin/kylin_sourcecode/dist/apache-kylin-3.0.0-alpha2-bin-hbase1x.tar.gz /home/admin
tar -zxvf /home/admin/apache-kylin-3.0.0-alpha2-bin-hbase1x.tar.gz
/home/admin/apache-kylin-3.0.0-alpha2-bin-hbase1x/kylin.sh start
```

We can also open pages for services such as Hdfs, Yarn, HBase, and Kylin in the browser of this machine.

## Container resource recommendation

In order to allow Kylin to build the cube smoothly, the memory resource we configured for Yarn NodeManager is 6G, plus the memory occupied by each service, please ensure that the memory of the container is not less than 8G, so as to avoid errors due to insufficient memory.

For the resource setting method for the container, please refer to:

- Mac user: <https://docs.docker.com/docker-for-mac/#advanced>
- Linux user: <https://docs.docker.com/config/containers/resource_constraints/#memory>

---

For old docker image, please check the github page [kylin-docker](https://github.com/Kyligence/kylin-docker/).