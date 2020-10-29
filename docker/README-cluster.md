# Kylin deployment by docker-compose for CI/CD

## Backgroud

In order to provide hadoop cluster(s) (without manual deployment) for system level testing to cover some complex features like read write speratation deployment, we would like to provide a docker-compose based way to make it easy to achieve CI/CD .

## Prepare

- Install the latest `docker` & `docker-compose`, following versions are recommended.
  - docker: 19.03.12+
  - docker-compose: 1.26.2+
  
- Install `python3` and test automation tool `gauge`, following versions are recommended.
  - python: 3.6+
  - gauge: 1.1.4+
 
- Check port 

    Port       |     Component     |     Comment
---------------| ----------------- | -----------------
    7070       |       Kylin       |      All     
    7071       |       Kylin       |      Job     
    7072       |       Kylin       |      Query             
    8088       |       Yarn        |       -    
    16010      |       HBase       |       -    
    50070      |       HDFS        |       -            

- Clone source code

```shell 
git clone
cd kylin/docker
```

### How to start Hadoop cluster

- Build and start a single Hadoop 2.8 cluster

```shell
bash setup_cluster.sh --s write --hadoop_version 2.8.5 --hive_version 1.2.2 \
    --enable_hbase yes --hbase_version 1.1.2  --enable_ldap nosh setup_cluster.sh --cluster_mode write \
    --hadoop_version 2.8.5 --hive_version 1.2.2 --enable_hbase yes --hbase_version 1.1.2  --enable_ldap no
```

## Docker Container

#### Docker Containers

- docker images

```shell 
root@open-source:/home/ubuntu/xiaoxiang.yu/docker# docker images | grep apachekylin
apachekylin/kylin-client                   hadoop_2.8.5_hive_1.2.2_hbase_1.1.2   728d1cd89f46        12 hours ago        2.47GB
apachekylin/kylin-hbase-regionserver       hbase_1.1.2                           41d3a6cd15ec        12 hours ago        1.13GB
apachekylin/kylin-hbase-master             hbase_1.1.2                           848831eda695        12 hours ago        1.13GB
apachekylin/kylin-hbase-base               hbase_1.1.2                           f6b9e2beb88e        12 hours ago        1.13GB
apachekylin/kylin-hive                     hive_1.2.2_hadoop_2.8.5               eb8220ea58f0        12 hours ago        1.83GB
apachekylin/kylin-hadoop-historyserver     hadoop_2.8.5                          f93b54c430f5        12 hours ago        1.63GB
apachekylin/kylin-hadoop-nodemanager       hadoop_2.8.5                          88a0f4651047        12 hours ago        1.63GB
apachekylin/kylin-hadoop-resourcemanager   hadoop_2.8.5                          32a58e854b6e        12 hours ago        1.63GB
apachekylin/kylin-hadoop-datanode          hadoop_2.8.5                          5855d6a0a8d3        12 hours ago        1.63GB
apachekylin/kylin-hadoop-namenode          hadoop_2.8.5                          4485f9d2beff        12 hours ago        1.63GB
apachekylin/kylin-hadoop-base              hadoop_2.8.5                          1b1605941562        12 hours ago        1.63GB
apachekylin/apache-kylin-standalone        3.1.0                                 2ce49ae43b7e        3 months ago        2.56GB
```

- docker containers

```shell
root@open-source:/home/ubuntu/xiaoxiang.yu/docker# docker ps
CONTAINER ID        IMAGE                                                          COMMAND                  CREATED             STATUS                             PORTS                                                        NAMES
4881c9b06eff        apachekylin/kylin-client:hadoop_2.8.5_hive_1.2.2_hbase_1.1.2   "/run_cli.sh"            8 seconds ago       Up 4 seconds                       0.0.0.0:7071->7070/tcp                                       kylin-job
4faed91e3b52        apachekylin/kylin-client:hadoop_2.8.5_hive_1.2.2_hbase_1.1.2   "/run_cli.sh"            8 seconds ago       Up 5 seconds                       0.0.0.0:7072->7070/tcp                                       kylin-query
b215230ab964        apachekylin/kylin-client:hadoop_2.8.5_hive_1.2.2_hbase_1.1.2   "/run_cli.sh"            8 seconds ago       Up 5 seconds                       0.0.0.0:7070->7070/tcp                                       kylin-all
64f77396e9fb        apachekylin/kylin-hbase-regionserver:hbase_1.1.2               "/opt/entrypoint/hba…"   12 seconds ago      Up 8 seconds                       16020/tcp, 16030/tcp                                         write-hbase-regionserver1
c263387ae9dd        apachekylin/kylin-hbase-regionserver:hbase_1.1.2               "/opt/entrypoint/hba…"   12 seconds ago      Up 10 seconds                      16020/tcp, 16030/tcp                                         write-hbase-regionserver2
9721df1d412f        apachekylin/kylin-hbase-master:hbase_1.1.2                     "/opt/entrypoint/hba…"   12 seconds ago      Up 9 seconds                       16000/tcp, 0.0.0.0:16010->16010/tcp                          write-hbase-master
ee859d1706ba        apachekylin/kylin-hive:hive_1.2.2_hadoop_2.8.5                 "/opt/entrypoint/hiv…"   20 seconds ago      Up 17 seconds                      0.0.0.0:10000->10000/tcp, 10002/tcp                          write-hive-server
b9ef97438912        apachekylin/kylin-hive:hive_1.2.2_hadoop_2.8.5                 "/opt/entrypoint/hiv…"   20 seconds ago      Up 16 seconds                      9083/tcp, 10000/tcp, 10002/tcp                               write-hive-metastore
edf687ecb3f0        mysql:5.7.24                                                   "docker-entrypoint.s…"   23 seconds ago      Up 20 seconds                      0.0.0.0:3306->3306/tcp, 33060/tcp                            metastore-db
7f63c83dcb63        zookeeper:3.4.10                                               "/docker-entrypoint.…"   25 seconds ago      Up 23 seconds                      2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp                   write-zookeeper
aaf514d200e0        apachekylin/kylin-hadoop-datanode:hadoop_2.8.5                 "/opt/entrypoint/had…"   28 seconds ago      Up 26 seconds                      50075/tcp                                                    write-datanode1
6a73601eba35        apachekylin/kylin-hadoop-datanode:hadoop_2.8.5                 "/opt/entrypoint/had…"   33 seconds ago      Up 28 seconds                      50075/tcp                                                    write-datanode3
934b5e7c8c08        apachekylin/kylin-hadoop-resourcemanager:hadoop_2.8.5          "/opt/entrypoint/had…"   33 seconds ago      Up 26 seconds (health: starting)   0.0.0.0:8088->8088/tcp                                       write-resourcemanager
6405614c2b06        apachekylin/kylin-hadoop-nodemanager:hadoop_2.8.5              "/opt/entrypoint/had…"   33 seconds ago      Up 30 seconds (health: starting)   8042/tcp                                                     write-nodemanager2
e004fc605295        apachekylin/kylin-hadoop-namenode:hadoop_2.8.5                 "/opt/entrypoint/had…"   33 seconds ago      Up 28 seconds (health: starting)   0.0.0.0:9870->9870/tcp, 8020/tcp, 0.0.0.0:50070->50070/tcp   write-namenode
743105698b0f        apachekylin/kylin-hadoop-historyserver:hadoop_2.8.5            "/opt/entrypoint/had…"   33 seconds ago      Up 29 seconds (health: starting)   0.0.0.0:8188->8188/tcp                                       write-historyserver
1b38135aeb2a        apachekylin/kylin-hadoop-nodemanager:hadoop_2.8.5              "/opt/entrypoint/had…"   33 seconds ago      Up 31 seconds (health: starting)   8042/tcp                                                     write-nodemanager1
7f53f5a84533        apachekylin/kylin-hadoop-datanode:hadoop_2.8.5                 "/opt/entrypoint/had…"   33 seconds ago      Up 29 seconds                      50075/tcp                                                    write-datanode2
``` 

- edit /etc/hosts to make it easy to view Web UI

```shell 
10.1.2.41 write-namenode
10.1.2.41 write-resourcemanager
10.1.2.41 write-hbase-master 
10.1.2.41 write-hive-server
10.1.2.41 write-hive-metastore
10.1.2.41 write-zookeeper
10.1.2.41 metastore-db
10.1.2.41 kylin-job
10.1.2.41 kylin-query
10.1.2.41 kylin-all
```

#### Hadoop cluster information

-  Support Matrix

Hadoop Version   |  Hive Version |  HBase Version |  Verified
---------------- | ------------- | -------------- | ----------
     2.8.5       |     1.2.2     |     1.1.2      |    Yes
     3.1.4       |     2.3.7     |     2.2.6      | In progress

- Component

   hostname          |                        URL                       |       Tag       |        Comment
------------------   | ------------------------------------------------ | --------------- | ------------------------
write-namenode       | http://write-namenode:50070                      |       HDFS      |
write-datanode1      |
write-datanode2      |
write-datanode3      |
write-resourcemanager| http://write-resourcemanager:8088/cluster        |       YARN      |
write-nodemanager1   | 
write-nodemanager2   |
write-historyserver  |
write-hbase-master   | http://write-hbase-master:16010                  |       HBase     |
write-hbase-regionserver1 |
write-hbase-regionserver2 |
write-hive-server    |                                                  |       Hive      |
write-hive-metastore |                                                  |       Hive      |
write-zookeeper      |                                                  |     Zookeeper   |
metastore-db         |                                                  |       RDBMS     |
kylin-job            | http://kylin-all:7071/kylin                      |       Kylin     |
kylin-query          | http://kylin-all:7072/kylin                      |       Kylin     |
kylin-all            | http://kylin-all:7070/kylin                      |       Kylin     |


## System Testing
### How to package kylin binary

```shell
cd dev-support/build-release
bash -x package.sh
``` 

### How to start Kylin

```shell 
## copy kylin into kylin/docker/docker-compose/others/kylin

cp kylin.tar.gz kylin/docker/docker-compose/others/kylin
tar zxf kylin.tar.gz

cp -r apache-kylin-bin/*  kylin/docker/docker-compose/others/kylin/kylin-all
cp -r apache-kylin-bin/* kylin/docker/docker-compose/others/kylin/kylin-job
cp -r apache-kylin-bin/* kylin/docker/docker-compose/others/kylin/kylin-query

## you can modify kylin/docker/docker-compose/others/kylin/kylin-*/kylin.properties before start kylin.

## start kylin

bash setup_service.sh --cluster_mode write --hadoop_version 2.8.5 --hive_version 1.2.2 \
      --enable_hbase yes --hbase_version 1.1.2  --enable_ldap nosh setup_cluster.sh \
      --cluster_mode write --hadoop_version 2.8.5 --hive_version 1.2.2 --enable_hbase yes \
      --hbase_version 1.1.2  --enable_ldap no
```

### How to run automated tests

```shell
cd build/CI/kylin-system-testing
pip install -r requirements.txt
gauge run
```

Wait for the test to complete, then you can check build/CI/kylin-system-testing/reports/html-report/index.html for reports.