---
layout: docs15
title:  "Deploy in cluster mode"
categories: install
permalink: /docs15/install/kylin_cluster.html
---


### Kylin Server modes

Kylin instances are stateless,  the runtime state is saved in its "Metadata Store" in hbase (kylin.metadata.url config in conf/kylin.properties). For load balance considerations it is possible to start multiple Kylin instances sharing the same metadata store (thus sharing the same state on table schemas, job status, cube status, etc.)

Each of the kylin instances has a kylin.server.mode entry in conf/kylin.properties specifying the runtime mode, it has three options: 1. "job" for running job engine only 2. "query" for running query engine only and 3 "all" for running both. Notice that only one server can run the job engine("all" mode or "job" mode), the others must all be "query" mode.

A typical scenario is depicted in the following chart:

![]( /images/install/kylin_server_modes.png)

### Setting up Multiple Kylin REST servers

If you are running Kylin in a cluster or you have multiple Kylin REST server instances, please make sure you have the following property correctly configured in ${KYLIN_HOME}/conf/kylin.properties

1. kylin.rest.servers 
	List of web servers in use, this enables one web server instance to sync up with other servers. For example: kylin.rest.servers=sandbox1:7070,sandbox2:7070
  
2. kylin.server.mode
	Make sure there is only one instance whose "kylin.server.mode" is set to "all" if there are multiple instances.
	
## Setup load balancer 

To enable Kylin high availability, you need setup a load balancer in front of these servers, let it routing the incoming requests to the cluster. Client sides send all requests to the load balancer, instead of talk with a specific instance. 
	