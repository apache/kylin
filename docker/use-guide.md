Docker for Kylin is convenient for users to quickly try Kylin, as well as for developers to debug and verify code modifications.

In the image, including:
* Jdk 1.8
* Hadoop 2.7.0
* Hive 1.2.1
* Hbase 1.1.2
* Spark 2.3.1
* Zookeeper 3.4.6
* Kafka 1.1.1
* Mysql
* Maven 3.6.1

## For user to try Kylin
The image has been pushed to the docker hub without having to build locally.
Execute the following command to pull image: 
```
docker pull codingforfun/apache-kylin-standalone
```
Then, execute:
> Please ensure that the container is allocated more than 8G of memory.
```
sh run_container.sh
```
to launch container which will automatically start the following services:
* Mysql
* NameNode
* DataNode
* ResourceManager
* NodeManager
* HMaster
* Kafka
* Kylin

## For developer to debug and verify
By default, 8G memory is configured for the yarn nodemanger. If this value cannot be provided, please modify the conf/hadoop/yarn-site.xml(config `yarn.nodemanager.resource.memory-mb`) before building the image.
To modify this value, don't be less than 4G, otherwise building cubes may fail.
### Step1: Build image
Execute the following command to build image
```
sh build_image.sh
```
This operation may last for several tens of minutes due to the download of the installation package.

### Step2: Launch container
After image built, execute
```
sh run_container.sh
```
to launch container which will automatically start the following services:
* Mysql
* NameNode
* DataNode
* ResourceManager
* NodeManager
* HMaster
* Kafka

Then execute command [docker exec](https://docs.docker.com/engine/reference/commandline/exec/) to run command in then running Kylin container.

When building image, the latest kylin source code will be copied to `/home/admin/kylin_sourcecode`.

Execute the following commands to do package:
```
cd /home/admin/kylin_sourcecode
build/script/package.sh
```
After the package is complete, use the package in `/home/admin/kylin_sourcecode/dist` to start the Kylin service.