# Kubernetes QuickStart

This guide shows how to run Kylin cluster using Kubernetes StatefulSet Controller. The following figure depicts a typical scenario for Kylin cluster mode deployment:

![image_name](http://kylin.apache.org/images/install/kylin_server_modes.png)

## Build or Pull Docker Image

You can pull the image from Docker Hub directly if you do not want to build the image locally:

```bash
docker pull apachekylin/apache-kylin:3.0.0-cdh57
```

TIPS: If you are woking with air-gapped network or slow internet speeds, we suggest you prepare the binary packages by yourself and execute this:

```bash
docker build -t "apache-kylin:${KYLIN_VERSION}-cdh57" --build-arg APACHE_MIRRORS=http://127.0.0.1:8000 .
```

## Prepare your Hadoop Configuration

Put all of the configuration files under the "conf" directory.

```bash
kylin.properties
applicationContext.xml  # If you need to set cacheManager to Memcached
hbase-site.xml
hive-site.xml
hdfs-site.xml
core-site.xml
mapred-site.xml
yarn-site.xml
```

If you worked with Kerberized Hadoop Cluster, do not forget to prepare the following files:

```bash
krb5.conf
kylin.keytab
```

## Create ConfigMaps and Secret

We recommand you to create separate Kubernetes namespace for Kylin.

```bash
kubectl create namespace kylin
```

Execute the following shell scripts to create the required ConfigMaps:

```bash
./kylin-configmap.sh
./kylin-secret.sh
```

## Create Service and StatefulSet

Make sure the following resources exist in your namespace:

```bash
kubectl get configmaps,secret -n kylin

NAME                      DATA   AGE
configmap/hadoop-config   4      89d
configmap/hbase-config    1      89d
configmap/hive-config     1      89d
configmap/krb5-config     1      89d
configmap/kylin-config    1      89d
configmap/kylin-context   1      45d

NAME                         TYPE                                  DATA   AGE
secret/kylin-keytab          Opaque                                1      89d

```

Then, you need to create headless service for stable DNS entries(kylin-0.kylin, kylin-1.kylin, kylin-2.kylin...) of StatefulSet members.

```bash
kubectl apply -f kylin-service.yaml
```

Finally, create the StatefulSet and try to use it:

```bash
kubectl apply -f kylin-job-statefulset.yaml
kubectl apply -f kylin-query-statefulset.yaml
```

If everything goes smoothly, you should see all 3 Pods become Running:

```bash
kubectl get statefulset,pod,service -n kylin

NAME                           READY   AGE
statefulset.apps/kylin-job     1/1     36d
statefulset.apps/kylin-query   3/3     36d

NAME                READY   STATUS    RESTARTS   AGE
pod/kylin-job-0     1/1     Running   0          13m
pod/kylin-query-0   1/1     Running   0          40h
pod/kylin-query-1   1/1     Running   0          40h

NAME                  TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)    AGE
service/kylin         ClusterIP   None             <none>        7070/TCP   58d
service/kylin-job     ClusterIP   xx.xxx.xx.xx     <none>        7070/TCP   89d
service/kylin-query   ClusterIP   xx.xxx.xxx.xxx   <none>        7070/TCP   89d
```
