# Welcome To Kylin 4 On Cloud Deployment Solution !

**Apache Kylin community** released Kylin 4.0 with a new architecture, which dedicated to building a high-performance and low-cost OLAP engine. The architecture of Kylin 4.0 supports the separation of storage and computing, which enables Kylin users to run Kylin 4.0 by adopting a more flexible and elastically scalable cloud deployment method.

For  the best practices of Kylin4 on the cloud,  **Apache Kylin community**  contribute a **tool** to deploy kylin4 clusters on **AWS** cloud easily and conveniently.

# Introduction About This Tool

## Features

1. Deploy a Kylin4 cluster on Ec2 with Spark Standalone mode in `10` minutes.
2. Support to scale nodes (Kylin & Spark Worker) quickly and conveniently.
3. Improve performance for query of Kylin4 in using  `Local Cache + Soft Affinity` feature (`Experimental Feature`), please check the [details](https://kylin.apache.org/blog/2021/10/21/Local-Cache-and-Soft-Affinity-Scheduling/).
4. Support to monitor status of cluster with `prometheus server` and `granfana`.

## Architecture

When cluster(s) created, services and nodes will like below:

![architecture](./images/structure.png)

- **Every stack module means related services will be controlled by a stack.** 
- **Read-write separated cluster will be easy created as same as image of architecture above.**
- Services are created as the number order from 1 to 5.
- Every machine node is presented by a white box. 
- `Kylin Node` and `Spark Worker` Node can be easy to scale.
- Whole clusters will have only one RDS and only one machine node which contains `Prometheus Server` and `Hive MetaStore` service.



## Quick Start

- Details about **`quick start`** of tool, please referer to [quick start](./readme/quick_start.md).
- Details about **`quick start for mutilple clusters`** of tool, please referer to [quick start for mutilple clusters](./readme/quick_start_for_multiple_clusters.md).



## Notes

1. More details about `commands` of tool, see document [commands](./readme/commands.md).
2. More details about `prerequisites` of tool, see document [prerequisites](./readme/prerequisites.md).
3. More details about `advanced configs` of tool, see document [advanced configs](./readme/advanced_configs.md).
4. More details about `monitor services` supported by tool, see document [monitor](./readme/monitor.md).
5. Current tool already open the public port for some services. You can access the service by `public ip` of related EC2 instance.
   1. `SSH`: 22.
   2. `Granfana`:  3000.
   3. `Prmetheus`:  9090, 9100.
   4. `Kylin`: 7070.
   5. `Spark`: 8080, 4040.
6. More about cloudformation syntax, please check [aws website](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html).
7. Current Kylin version is 4.0.0.
8. Current Spark version is 3.1.1.
