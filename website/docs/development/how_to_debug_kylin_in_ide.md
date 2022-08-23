---
title: How to debug Kylin in IDEA
language: en
sidebar_label: How to debug
pagination_label: How to debug
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
- developer
- debug
draft: false
last_update:
date: 08/23/2022
---

# How to debug Kylin in IDEA using docker

### Prepare IDEA and build source code
#### Build source code
- Build back-end source code before your start debug.
```shell
mvn clean install -DskipTests
```

- Build front-end source code. 
(Please use node.js v12.14.0, for how to use specific version of node.js, please check [how to package](./how_to_package.md) )
```shell
cd kystudio
npm install
```

#### Prepare IDEA configuration
- Download spark and create running IDEA configuration for debug purpose.
```shell
./dev-support/sandbox/sandbox.sh init
```

- Following is the shell output.

![sandbox.sh init](images/how-to-debug-01.png)

### Prepare the Hadoop Cluster

#### Deploy Hadoop Cluster
- Install latest docker desktop in your laptop
- [**Optional**] Install docker engine on remote machine (https://docs.docker.com/engine/install/)
- [**Optional**] If you want to deploy hadoop cluster on remote machine, please set correct `DOCKER_HOST`

```shell
# see more detail at : https://docs.docker.com/compose/reference/envvars/#docker_host
export DOCKER_HOST=ssh://${USER}@${DOCKER_HOST}
```

- Deploy hadoop cluster via docker compose on laptop(or on remote machine)

```shell
./dev-support/sandbox/sandbox.sh up
```

![sandbox.sh up](images/how-to-debug-02.png)


#### Check status of hadoop cluster
- Wait 1-2 minutes, check health of hadoop, you can use following command to check status

```shell
./dev-support/sandbox/sandbox.sh ps
```

Following output content shows all hadoop component are in health state.

![sandbox.sh ps](images/how-to-debug-03.png)

- Edit `/etc/hosts`. (if you deployed Hadoop cluster on remote machine, please use correct ip address other than `127.0.0.1` .)
```shell
127.0.0.1 namenode datanode resourcemanager nodemanager historyserver mysql zookeeper hivemetastore hiveserver 
```

- Load sample SSB data into HDFS and Hive
```shell
./dev-support/sandbox/sandbox.sh sample
```

- Check hive table

![hive table](images/how-to-debug-04.png)

### Debug Kylin in IDEA

#### Deployment architecture
- Following is architecture of current deployment.

![debug_in_laptop](images/debug_kylin_by_docker_compose.png)


#### Start backend in IDEA

- Select "BootstrapServer[docker-sandbox]" on top of IDEA and click **Run** .

![click BootstrapServer[docker-sandbox]](images/how-to-debug-05.png)

- Wait and check if Sparder is start succeed.

![Spark is submitted](images/how-to-debug-06.png)

- Check if SparkUI of Sparder is started.

![Spark UI](images/how-to-debug-07.png)


#### Start frontend in IDEA

- Set up dev proxy
```shell
cd kystudio
npm run devproxy
```

![steup front end](images/how-to-debug-08.png)

- Visit Kylin WEB UI in your laptop.

![steup front end](images/how-to-debug-09.png)


### Command manual
1. Use `./dev-support/sandbox/sandbox.sh stop` to stop all containers
2. Use `./dev-support/sandbox/sandbox.sh start` to start all containers
3. Use `./dev-support/sandbox/sandbox.sh ps` to check status of all containers
4. Use `./dev-support/sandbox/sandbox.sh down` to stop all containers and delete them

## Q&A

// todo
