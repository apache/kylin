---
layout: docs
title:  "Multiple Kylin REST servers"
categories: install
permalink: /docs/install/kylin_cluster.html
version: v0.7.2
since: v0.7.1
---

>version: {{page.version}}, since {{page.since}}

### Multiple Kylin REST servers

If you are running Kylin in a cluster or you have multiple Kylin REST server instances, please make sure you have the following property correctly configured in ${KYLIN_HOME}/conf/kylin.properties

1. kylin.rest.servers 
	List of web servers in use, this enables one web server instance to sync up with other servers.
  
2. kylin.server.mode
	Make sure there is only one instance whose "kylin.server.mode" is set to "all" if there are multiple instances.