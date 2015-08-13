---
layout: docs
title:  "On Hadoop Kylin installation using Docker"
categories: install
permalink: /docs/install/kylin_docker.html
version: v0.6
since: v0.6
---

With help of SequenceIQ, we have put together a fully automated method of creating a Kylin cluster (along with Hadoop, HBase and Hive). The only thing you will need to do is to pull the container from the official Docker repository by using the commands listed below:

### Pre-Requisite

1. Docker (If you don't have Docker installed, follow this [link](https://docs.docker.com/installation/#installation))
2. Minimum RAM - 4Gb (We'll be running Kylin, Hadoop, HBase & Hive)

### Installation
{% highlight Groff markup %}
docker pull sequenceiq/kylin:0.7.2
{% endhighlight %}

Once the container is pulled you are ready to start playing with Kylin. Get the following helper functions from our Kylin GitHub [repository](https://github.com/sequenceiq/docker-kylin/blob/master/ambari-functions) - _(make sure you source it)._

{% highlight Groff markup %}
 $ wget https://raw.githubusercontent.com/sequenceiq/docker-kylin/master/ambari-functions
 $ source ambari-functions
{% endhighlight %}
{% highlight Groff markup %}
 $ kylin-deploy-cluster 1
{% endhighlight %}

You can specify the number of nodes you'd like to have in your cluster (1 in this case). Once we installed all the necessary Hadoop
services we'll build Kylin on top of it and then you can reach the UI on: 
{% highlight Groff markup %}
#Ambari Dashboard
http://<container_ip>:8080
{% endhighlight %}
Use `admin/admin` to login. Make sure HBase is running. 

{% highlight Groff markup %}
#Kylin Dashboard
http://<container_ip>:7070/kylin
{% endhighlight %}
The default credentials to login are: `ADMIN:KYLIN`. 
The cluster is pre-populated with sample data and is ready to build cubes as shown [here](../tutorial/create_cube.html).
  
