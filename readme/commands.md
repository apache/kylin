## Commands<a name="run"></a>

Command:

```shell
python deploy.py --type [deploy|destroy|list|scale] --scale-type [up|down] --node-type [kylin|spark_worker] [--cluster {1..6}|all|default]
```

- deploy: create cluster(s).

- destroy: destroy created cluster(s).

- list: list alive nodes which are with stack name, instance id, private ip and public ip.

- scale: Must be used with `--scale-type` and `--node-type`.

  > Note:
  >
  > 1. Current support to scale up/down `kylin` or `spark_worker` for specific cluster.
  > 2. Before scale up/down `kylin` or `spark_worker` nodes, Cluster services must be ready.
  > 3. If you want to scale a `kylin` or `spark_worker` node to a specify cluster, please add the `--cluster ${cluster num}` to specify the expected node add to the cluster `${cluster num}`.

### Command for deploy

- Deploy default cluster

```shell
$ python deploy.py --type deploy [--cluster default]
```

- Deploy a cluster with specific cluster index. 

```shell
$ python deploy.py --type deploy --cluster ${cluster num}
```

> Note: the `${cluster num}` must be in the range of `CLUSTER_INDEXES`.

- Deploy all cluster which contain default cluster and all cluster which index in the range of `CLUSTER_INDEXES`.

```shell
$ python deploy.py --type deploy --cluster all
```

### Command for destroy

> Note:
>
> ​		Destroy all cluster will not delete vpc, rds and monitor node. So if user don't want to hold the env, please set the `ALWAYS_DESTROY_ALL` to be `'true'`.

- Destroy default cluster

```shell
$ python deploy.py --type destroy [--cluster default]
```

- Destroy a cluster with specific cluster index. 

```shell
$ python deploy.py --type destroy --cluster ${cluster num}
```

> Note: the `${cluster num}` must be in the range of `CLUSTER_INDEXES`.

- Destroy all cluster which contain default cluster and all cluster which index in the range of `CLUSTER_INDEXES`.

```shell
$ python deploy.py --type destroy --cluster all
```

### Command for list

- List nodes which are with **stack name**, **instance id**, **private ip** and **public ip** in **available stacks** .

```shell
$ python deploy.py --type list
```

### Command for scale

> Note:
>
> 1. Scale command must be used with `--scale-type` and `--node-type`.
> 2. If scale command not specify a cluster num, then the scaled node(kylin or spark worker) will be add to `default`cluster.
> 3. Scale command **not support** to **scale** node (kylin or spark worker) to **all clusters** at **one time**. It means that `python ./deploy.py --type scale --scale-type up[|down] --node-type kylin[|spark_worker] --cluster all` is invalid commad.
> 4. Scale params which are `KYLIN_SCALE_UP_NODES`, `KYLIN_SCALE_DOWN_NODES`, `SPARK_WORKER_SCALE_UP_NODES` and `SPARK_WORKER_SCALE_DOWN_NODES` effect on all cluster. So if user want to scale node for a specify cluster, then modify the scale params before **every run time.**
> 5. **(Important!!!)** Current cluster is created with default `3` spark workers and `1` kylin node. The `3` spark workers can not be scaled down. The `1`  kylin node also can not be scaled down.
> 6. **(Important!!!)** Cluster can only scale up or down the range of nodes which is in  `KYLIN_SCALE_UP_NODES`, `KYLIN_SCALE_DOWN_NODES`, `SPARK_WORKER_SCALE_UP_NODES` and `SPARK_WORKER_SCALE_DOWN_NODES` . Not the default `3` spark workers and `1` kylin node in the cluster.
> 7. **(Important!!!)**  If user don't want to create a cluster with `3` default spark workers, then user can remove the useless node module in the `Ec2InstanceOfSlave0*` of `cloudformation_templates/ec2-cluster-spark-slave.yaml`. User need to know about the syntax of cloudformation as also.

- Scale up/down kylin/spark workers in default cluster

```shell
python deploy.py --type scale --scale-type up[|down] --node-type kylin[|spark_worker] [--cluster default]
```

- Scale up/down kylin/spark workers in a specific cluster

```shell
python deploy.py --type scale --scale-type up[|down] --node-type kylin[|spark_worker] --cluster ${cluster num}
```

> Note: the `${cluster num}` must be in the range of `CLUSTER_INDEXES`.