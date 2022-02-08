# Welcome To Kylin 4 On Cloud Deployment Solution!

**Apache Kylin community** released Kylin 4.0 with a new architecture, which is dedicated to building a high-performance and low-cost OLAP engine. The architecture of Kylin 4.0 supports the separation of storage and computing, which enables Kylin users to run Kylin 4.0 by adopting a more flexible and elastically scalable cloud deployment method.

For the best practices of Kylin4 on the cloud,  **Apache Kylin community contributes a **tool** to deploy kylin4 clusters on **AWS** cloud easily and conveniently.

# Introduction About This Tool

## Features

1. Deploy a Kylin4 cluster on Ec2 with Spark Standalone mode in `10` minutes.
2. Support to scale nodes (Kylin & Spark Worker) quickly and conveniently.
3. Improve performance for the query of Kylin4 in using  `Local Cache + Soft Affinity` feature (`Experimental Feature`), please check the [details](https://kylin.apache.org/blog/2021/10/21/Local-Cache-and-Soft-Affinity-Scheduling/).
4. Support to monitor the status of the cluster with the `Prometheus server` and `Granfana`.

## Architecture

When cluster(s) created, services and nodes will be like below:

![architecture](./images/structure.png)

- **Every stack module means related services will be controlled by a stack.** 
- The **read-write separated cluster will be easily created as same as the image of architecture above.**
- Services are created as the number order from 1 to 5.
- Every machine node is presented by a white box. 
- `Kylin Node` and `Spark Worker` Node can be easy to scale.
- Whole clusters will have only one RDS and only one machine node which contains `Prometheus Server` and `Hive MetaStore` service.



## Quick Start

- Details about **`quick start`** of tool, please referer to [quick start](./readme/quick_start.md).
- Details about **`quick start for multiple clusters`** of tool, please referer to [quick start for multiple clusters](./readme/quick_start_for_multiple_clusters.md).



## Notes

1. For more details about `cost` of tool, see document [cost calculation](./readme/cost_calculation.md).
2. For more details about `commands` of tool, see document [commands](./readme/commands.md).
3. For more details about the `prerequisites` of tool, see document [prerequisites](./readme/prerequisites.md).
4. For more details about `advanced configs` of tool, see document [advanced configs](./readme/advanced_configs.md).
5. For more details about `monitor services` supported by tool, see document [monitor](./readme/monitor.md).
6. For more details about `troubleshooting`, see document [troubleshooting](./readme/trouble_shooting.md).
7. The current tool has already opened the public port for some services. You can access the service by `public IP` of related EC2 instances.
   1. `SSH`: 22.
   2. `Granfana`:  3000.
   3. `Prometheus`:  9090, 9100.
   4. `Kylin`: 7070.
   5. `Spark`: 8080, 4040.
8. More about cloudformation syntax, please check [aws website](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html).
9. The current Kylin version is 4.0.0.
10. The current Spark version is 3.1.1.
