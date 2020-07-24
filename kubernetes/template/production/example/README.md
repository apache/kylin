## Demo in CDH5.7

> This doc will demonstrate how to apply production deployment template, so all **required** configuration files are provided in `config` directory. 
> For CDH5.x user, I think you can simply replace hadoop configuration file with yours and executing the following 
> commands to deploy a kylin cluster very quickly.

### Pre-requirements

- A healthy CDH 5.7 hadoop cluster.
- A healthy on-premise, latest version K8s cluster, with at least 8 available cores and 20 GB memory.
- A health Elasticsearch cluster.

### Deploy steps

- Create statefulset and service for memcached
```
root@open-source:/home/ubuntu/example/deployment# kubectl apply -f memcached/
service/cache-svc created
statefulset.apps/kylin-memcached created
```


- Check hostname of cache service
```shell
root@open-source:/home/ubuntu# kubectl run -it --image=busybox:1.28.4 --rm --restart=Never sh -n test-dns
If you don't see a command prompt, try pressing enter.

/ # nslookup cache-svc.kylin-example.svc.cluster.local
Server:    10.96.0.10
Address 1: 10.96.0.10 kube-dns.kube-system.svc.cluster.local

Name:      cache-svc.kylin-example.svc.cluster.local
Address 1: 192.168.11.44 kylin-memcached-0.cache-svc.kylin-example.svc.cluster.local
/ #
```

- Modify memcached configuration
```shell
# modify memcached hostname(session sharing)
# memcachedNodes="n1:kylin-memcached-0.cache-svc.kylin-example.svc.cluster.local:11211"
vim ../config/tomcat/context.xml


# modify memcached hostname(query cache)
# kylin.cache.memcached.hosts=kylin-memcached-0.cache-svc.kylin-example.svc.cluster.local:11211
vim ../config/kylin-job/kylin.properties
vim ../config/kylin-query/kylin.properties
```

- Create configMap
```shell
root@open-source:/home/ubuntu/example/deployment# kubectl create configmap -n kylin-example hadoop-config \
>     --from-file=../config/hadoop/core-site.xml \
>     --from-file=../config/hadoop/hdfs-site.xml \
>     --from-file=../config/hadoop/yarn-site.xml \
>     --from-file=../config/hadoop/mapred-site.xml \
>     --dry-run -o yaml | kubectl apply -f -
W0426 07:45:45.742257   19657 helpers.go:535] --dry-run is deprecated and can be replaced with --dry-run=client.
configmap/hadoop-config created

root@open-source:/home/ubuntu/example/deployment# kubectl create configmap -n kylin-example hive-config \
>     --from-file=../config/hadoop/hive-site.xml \
>     --dry-run -o yaml | kubectl apply -f -
W0426 07:45:54.889003   19864 helpers.go:535] --dry-run is deprecated and can be replaced with --dry-run=client.
configmap/hive-config created

root@open-source:/home/ubuntu/example/deployment# kubectl create configmap -n kylin-example hbase-config \
>     --from-file=../config/hadoop/hbase-site.xml \
>     --dry-run -o yaml | kubectl apply -f -
W0426 07:46:04.623956   20071 helpers.go:535] --dry-run is deprecated and can be replaced with --dry-run=client.
configmap/hbase-config created

root@open-source:/home/ubuntu/example/deployment# kubectl create configmap -n kylin-example kylin-more-config \
>     --from-file=../config/kylin-more/applicationContext.xml \
>     --from-file=../config/kylin-more/ehcache.xml \
>     --from-file=../config/kylin-more/ehcache-test.xml \
>     --from-file=../config/kylin-more/kylinMetrics.xml \
>     --from-file=../config/kylin-more/kylinSecurity.xml \
>     --dry-run -o yaml | kubectl apply -f -
W0426 08:02:13.170807    5454 helpers.go:535] --dry-run is deprecated and can be replaced with --dry-run=client.

root@open-source:/home/ubuntu/example/deployment# kubectl create configmap -n kylin-example kylin-job-config  \
>     --from-file=../config/kylin-job/kylin-kafka-consumer.xml \
>     --from-file=../config/kylin-job/kylin_hive_conf.xml \
>     --from-file=../config/kylin-job/kylin_job_conf.xml \
>     --from-file=../config/kylin-job/kylin_job_conf_inmem.xml \
>     --from-file=../config/kylin-job/kylin-server-log4j.properties \
>     --from-file=../config/kylin-job/kylin-spark-log4j.properties \
>     --from-file=../config/kylin-job/kylin-tools-log4j.properties \
>     --from-file=../config/kylin-job/kylin.properties \
>     --from-file=../config/kylin-job/setenv.sh \
>     --from-file=../config/kylin-job/setenv-tool.sh \
>     --dry-run -o yaml | kubectl apply -f -
W0426 08:02:35.168886    5875 helpers.go:535] --dry-run is deprecated and can be replaced with --dry-run=client.
configmap/kylin-job-config created

root@open-source:/home/ubuntu/example/deployment# kubectl create configmap -n kylin-example filebeat-config  \
>     --from-file=../config/filebeat/filebeat.yml \
>     --dry-run -o yaml | kubectl apply -f -
W0426 08:07:08.983369   10722 helpers.go:535] --dry-run is deprecated and can be replaced with --dry-run=client.
configmap/filebeat-config created

root@open-source:/home/ubuntu/example/deployment# kubectl create configmap -n kylin-example tomcat-config  \
>     --from-file=../config/tomcat/server.xml \
>     --from-file=../config/tomcat/context.xml \
>     --dry-run -o yaml | kubectl apply -f -
W0426 08:07:48.439995   11459 helpers.go:535] --dry-run is deprecated and can be replaced with --dry-run=client.
configmap/tomcat-config created
```

- Deploy Kylin's Job server(two instances)
```shell
root@open-source:/home/ubuntu/example/deployment# kubectl apply -f kylin-job/
service/kylin-job-svc created
statefulset.apps/kylin-job created
```


- Deploy Kylin's Query server(two instances)
```shell
root@open-source:/home/ubuntu/example/deployment# kubectl apply -f kylin-query/
service/kylin-query-svc created
statefulset.apps/kylin-query created
```

- Visit Web UI
1. `http://${POD_HOSTNAME}:30010/kylin` for JobServer 
2. `http://${POD_HOSTNAME}:30012/kylin` for QueryServer

### Cleanup
```shell
root@open-source:/home/ubuntu/example/deployment# kubectl delete -f memcached/
root@open-source:/home/ubuntu/example/deployment# kubectl delete -f kylin-query/
root@open-source:/home/ubuntu/example/deployment# kubectl delete -f kylin-job/
```

### Troubleshooting
- Check logs of specific pod
```shell
##  Output of : sh kylin.sh start
root@open-source:/home/ubuntu/example/deployment# kubectl logs kylin-job-0 kylin -n kylin-example

root@open-source:/home/ubuntu/example/deployment# kubectl logs -f kylin-job-0 kylin -n kylin-example
```

- Attach to a specific pod, say "kylin-job-0"
```shell
root@open-source:/home/ubuntu/example/deployment# kubectl exec -it  kylin-job-0  -n kylin-example -- bash
```

- Check failure reasons of specific pod
```shell
root@open-source:/home/ubuntu/example/deployment# kubectl get pod kylin-job-0  -n kylin-example -o yaml

root@open-source:/home/ubuntu/example/deployment# kubectl describe pod kylin-job-0  -n kylin-example
```

- Check if all status is Running
```shell
root@open-source:/home/ubuntu/example/deployment# kubectl get statefulset -n kylin-example
NAME              READY   AGE
kylin-memcached   1/1     45s

root@open-source:/home/ubuntu/example/deployment# kubectl get service -n kylin-example
NAME        TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)     AGE
cache-svc   ClusterIP   None         <none>        11211/TCP   54s

root@open-source:/home/ubuntu/example/deployment# kubectl get pod -n kylin-example
NAME                READY   STATUS    RESTARTS   AGE
kylin-memcached-0   1/1     Running   0          61s
```

- If you don't have a Elasticsearch cluster or not interested in log collection, please remove `filebeat` container in both `kylin-query-stateful.yaml` and `kylin-job-stateful.yaml`.