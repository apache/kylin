## Commands<a name="run"></a>

Command:
  > Note:
  >
  > Options are placed in `[]`, and different options are separated by `|`.

```shell
python deploy.py --type [deploy|destroy|destroy-all|list|scale] --mode [all|job|query] --scale-type [up|down] --node-type [kylin|spark_worker] --cluster [{1..6}|all|default]
```

- deploy: create cluster(s).

- destroy: destroy created cluster(s). Including kylin node, spark master node, spark slave node and zookeeper node.

- destroy-all: destroy all of the node. Including kylin node, spark master node, spark slave node,  zookeeper node, rds node, monitor node and vpc node .

- list: list alive nodes which are with node name, instance id, private IP, and public IP.

- scale: Must be used with `--scale-type` and `--node-type`.

  > Note:
  >
  > 1. Current support to scale up/down `kylin` or `spark_worker` for a specific cluster.
  > 2. Before scaling up/down `kylin` or `spark_worker` nodes, Cluster services must be ready.
  > 3. If you want to scale a `kylin` or `spark_worker` node to a specified cluster, please add the `--cluster ${cluster ID}` to specify the expected node add to the cluster `${cluster ID}`.
  > 4. For details about the index of the cluster,  please check [Indexes of clusters](./configuration.md#indexofcluster).

### Command for deploy

- Deploy a cluster, the mode of kylin node is `all`

```shell
$ python deploy.py --type deploy
```

- deploy a cluster, the mode of kylin node is `job`

```shell
$ python deploy.py --type deploy --mode job
```

- deploy a cluster, the mode of kylin node is `query`

```shell
$ python deploy.py --type deploy --mode query
```

- Deploy a cluster with a specific cluster index. <a name="deploycluster"></a>

```shell
$ python deploy.py --type deploy --cluster ${cluster ID}
```

> Note: the `${cluster ID}` must be in the range of `CLUSTER_INDEXES`.

- Deploy all of the clusters which contain the default cluster and all of the clusters whose index is in the range of `CLUSTER_INDEXES`.

```shell
$ python deploy.py --type deploy --cluster all
```

### Command for destroy

> Note:
>
> ​		By default, using the `destroy` command does not vpc, rds, and monitor node. So if user doesn't want to hold the env, please use `destroy-all` command.

- Destroy the default cluster

```shell
$ python deploy.py --type destroy
```

- Destroy a cluster with a specific cluster index. 

```shell
$ python deploy.py --type destroy --cluster ${cluster ID}
```

> Note: the `${cluster ID}` must be in the range of `CLUSTER_INDEXES`.

- Destroy all of the clusters which contain the default cluster and all of the clusters whose index is in the range of `CLUSTER_INDEXES`.

```shell
$ python deploy.py --type destroy --cluster all
```

### Command for list

- List nodes that are with **node name**, **instance id**, **private IP,** and **public IP** in **available stacks**.

```shell
$ python deploy.py --type list
```

### Command for scale

> Note:
>
> 1. Scale command must be used with `--scale-type` and `--node-type`.
> 2. If the scale command does not specify a `cluster ID`, then the scaled node(Kylin or spark worker) will be added to the `default` cluster.
> 3. Scale command **not support** to **scale** node (kylin or spark worker) to **all of the clusters** at **one time**. It means that `python ./deploy.py --type scale --scale-type up[|down] --node-type kylin[|spark_worker] --cluster all` is invalid commad.
> 4. Scale params which are `KYLIN_SCALE_UP_NODES`, `KYLIN_SCALE_DOWN_NODES`, `SPARK_WORKER_SCALE_UP_NODES` and `SPARK_WORKER_SCALE_DOWN_NODES` effect on all cluster. So if user wants to scale a node for a specific cluster, then modify the scale params before **every run time.**
> 5. **(Important!!!)** The current cluster is created with default `3` spark workers and `1` Kylin node. The `3` spark workers can not be scaled down. The `1`  Kylin node also can not be scaled down.
> 6. **(Important!!!)** The current cluster can only scale up or down the range of nodes which is in  `KYLIN_SCALE_UP_NODES`, `KYLIN_SCALE_DOWN_NODES`, `SPARK_WORKER_SCALE_UP_NODES,` and `SPARK_WORKER_SCALE_DOWN_NODES`. Not the default `3` spark workers and `1` kylin node in a cluster.
> 7. **(Important!!!)**  If user doesn't want to create a cluster with `3` default spark workers, then user can remove the useless node module in the `Ec2InstanceOfSlave0*` of `cloudformation_templates/ec2-cluster-spark-slave.yaml`. User needs to know about the syntax of `cloudformation` as also.

- Scale up/down Kylin/spark workers in the default cluster

```shell
python deploy.py --type scale --scale-type up[|down] --node-type kylin[|spark_worker] [--cluster default]
```

- Scale up/down kylin/spark workers in a specific cluster

```shell
python deploy.py --type scale --scale-type up[|down] --node-type kylin[|spark_worker] --cluster ${cluster ID}
```

> Note: the `${cluster ID}` must be in the range of `CLUSTER_INDEXES`.