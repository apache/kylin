## Background
Kubernetes is a portable, extensible, open-source platform for managing containerized workloads and services, that facilitates 
both declarative configuration and automation. It has a large, rapidly growing ecosystem. Kubernetes services, support, 
and tools are widely available.

Apache Kylin is a open source, distributed analytical data warehouse for big data. Deploy Kylin on Kubernetes 
cluster, will reduce cost of maintenance and extension.

### Directory Introduction
- **config**
  Please update your configuration file here. 
- **template**
  This directory provided two deployment templates, one for **quick-start** purpose, another for **production/distributed** deployment.
  1. Quick-start template is for one node deployment with an **ALL** kylin instance for test or PoC purpose.
  2. Production template is for multi-nodes deployment with a few of **job**/**query** kylin instances; and some other service 
  like **memcached** and **filebeat** will help to satisfy log collection/query cache/session sharing demand.
- **docker**
  Docker image is the pre-requirement of Kylin on Kubernetes, please check this directory if you need build it yourself.
  For CDH5.x user, you may consider use a provided image on DockerHub.
- **template/production/example**
  This is a complete example by applying production template in a CDH 5.7 hadoop env with step by step guide.  
 
### Note 
1. **CuratorScheduler** is used as default JobScheduler because it is more flexible.
2. Spark building require use `cluster` as deployMode. If you forget it, your spark application will never submitted successfully because Hadoop cluster can not resolve hostname of Pod (Spark Driver).
3. To modify `/etc/hosts` in Pod, please check this : https://kubernetes.io/docs/concepts/services-networking/add-entries-to-pod-etc-hosts-with-host-aliases/ . 
4. To build you own kylin-client docker image, please don't forget to download and put following jars into `KYLIN_HOME/tomcat/lib` to enable tomcat session sharing.
    - https://repo1.maven.org/maven2/de/javakaffee/msm/memcached-session-manager-tc7/2.1.1/
    - https://repo1.maven.org/maven2/de/javakaffee/msm/memcached-session-manager/2.1.1/
5. If you have difficulty in configure filebeat, please check this https://www.elastic.co/guide/en/beats/filebeat/current/index.html .
6. External query cache is enabled by default, if you are interested in detail, you may check http://kylin.apache.org/blog/2019/07/30/detailed-analysis-of-refine-query-cache/ .
7. All configuration files is separated from Docker image, please use **configMap** or **secret**. Compared to **configMap**, **secret** is more recommended for security reason.
8. Some verified kylin-client image will be published to DockerHub, here is the link https://hub.docker.com/r/apachekylin/kylin-client . You may consider contributed your `Dockerfile` to kylin's repo if you are interested.
 
### Reference 
- JIRA ticket: https://issues.apache.org/jira/browse/KYLIN-4447
- DockerHub: https://hub.docker.com/r/apachekylin/kylin-client