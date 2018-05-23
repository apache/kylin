---
layout: docs23
title:  "Deploy in Cluster Mode"
categories: install
permalink: /docs23/install/kylin_cluster.html
---


### Kylin Server modes

Kylin instances are stateless, the runtime state is saved in its metadata store in HBase ("kylin.metadata.url" config in conf/kylin.properties). For load balance considerations it is possible to start multiple Kylin instances sharing the same metadata store (thus sharing the same state on table schemas, job status, Cube status, etc.)

Each of the Kylin instances has a "kylin.server.mode" entry in conf/kylin.properties specifying the runtime mode, it has three options: 
 * "job" : for running job engine only;
 * "query" : for running query engine only;
 * "all" : for running both job and query engines. 

 Notice that only one server can run the job engine ("all" or "job" mode), the others must all be "query" mode.

A typical scenario is depicted in the following chart:

![]( /images/install/kylin_server_modes.png)

### Configure Multiple Kylin Servers

If you are running Kylin in a cluster where you have multiple Kylin REST server instances, please make sure you have the following property correctly configured in ${KYLIN_HOME}/conf/kylin.properties for EVERY server instance.

1. kylin.rest.servers 
	List of web servers in use, this enables one web server instance to sync up with other servers. For example: 

```
kylin.rest.servers=host1:7070,host2:7070
```

2. kylin.server.mode
	Make sure there is only one instance whose "kylin.server.mode" is set to "all"(or "job"), others should be "query"
	
## Setup Load Balancer 

To enable Kylin high availability, you need setup a load balancer in front of these servers, letting it routes the incoming requests to the cluster. Client sides send all requests to the load balancer, instead of talk with a specific instance. The setup of load balancer is out of the scope; you can select any implementation, like Nginx, F5 or any cloud LB service. 
	
