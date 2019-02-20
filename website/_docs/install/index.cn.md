---
layout: docs-cn
title:  "安装指南"
categories: install
permalink: /cn/docs/install/index.html
---

### 软件要求

* Hadoop: 2.7+, 3.1+ (since v2.5)
* Hive: 0.13 - 1.2.1+
* HBase: 1.1+, 2.0 (since v2.5)
* Spark (可选) 2.1.1+
* Kafka (可选) 1.0.0+ (since v2.5)
* JDK: 1.8+ (since v2.5)
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

在 Hortonworks HDP 2.2-2.6 and 3.0, Cloudera CDH 5.7-5.11 and 6.0, AWS EMR 5.7-5.10, Azure HDInsight 3.5-3.6 上测试通过。

我们建议您使用集成的 sandbox 来试用 Kylin 或进行开发，比如 [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/)，且要保证其有至少 10 GB 内存。在配置沙箱时，我们推荐您使用 Bridged Adapter 模型替代 NAT 模型。



### 硬件要求

运行 Kylin 的服务器的最低配置为 4 core CPU，16 GB 内存和 100 GB 磁盘。 对于高负载的场景，建议使用 24 core CPU，64 GB 内存或更高的配置。



### Hadoop 环境

Kylin 依赖于 Hadoop 集群处理大量的数据集。您需要准备一个配置好 HDFS，YARN，MapReduce,，Hive， HBase，Zookeeper 和其他服务的 Hadoop 集群供 Kylin 运行。
Kylin 可以在 Hadoop 集群的任意节点上启动。方便起见，您可以在 master 节点上运行 Kylin。但为了更好的稳定性，我们建议您将 Kylin 部署在一个干净的 Hadoop client 节点上，该节点上 Hive，HBase，HDFS 等命令行已安装好且 client 配置（如 `core-site.xml`，`hive-site.xml`，`hbase-site.xml`及其他）也已经合理的配置且其可以自动和其它节点同步。

运行 Kylin 的 Linux 账户要有访问 Hadoop 集群的权限，包括创建/写入 HDFS 文件夹，Hive 表， HBase 表和提交 MapReduce 任务的权限。 



### Kylin 安装

1. 从 [Apache Kylin下载网站](https://kylin.apache.org/download/) 下载一个适用于您 Hadoop 版本的二进制文件。例如，适用于 HBase 1.x 的 Kylin 2.5.0 可通过如下命令行下载得到：

```shell
cd /usr/local/
wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-2.5.0/apache-kylin-2.5.0-bin-hbase1x.tar.gz
```

2. 解压 tar 包，配置环境变量 `$KYLIN_HOME` 指向 Kylin 文件夹。

```shell
tar -zxvf apache-kylin-2.5.0-bin-hbase1x.tar.gz
cd apache-kylin-2.5.0-bin-hbase1x
export KYLIN_HOME=`pwd`
```

### Kylin tar 包解压后目录
* `bin` 目录下主要是一些 shell 脚本，包括查找 hbase、hive、kafka、spark 依赖的文件，检查端口、hive 可用性文件
* `conf` 目录下主要是一些 xml 配置文件，这些 xml 文件的作用可参考[这个页面](http://kylin.apache.org/docs/install/configuration.html)
* `lib` 目录下主要是一些 jar 包
* `meta_backups` 这个目录会在备份了元数据后出现，其下一级目录会按照不同备份时间分为不同的目录，命名为 `meta_year_month_day_hour_minute_second`，而该目录下又有 `acl`，`cube`，`cube_desc`，`dict`，`execute`，`execute_output`，`model_desc`, `project`，`table`，`table_exd`，`user`，`UUID` 这些目录及文件。
　1. `acl` 目录下有一个文件，存有 uuid，last_modified，version，domainObjectInfo(type，id），parentDomainObjectInfo, ownerInfo(sid, principal), entriesInheriting 和 entries（p,m）信息；
　2. `cube` 下存放的是该 metadata 下的 cube 构建信息，对应为 cube_name.json，包括 segment 信息以及 cube 的状态等信息；
　3. `cube_desc` 下存放的是该 metadata 下 cube 创建的信息，不同的 cube 对应为不同的 cube_name.json，包括维度，度量，字典，rowkey 聚合组，partition 起始时间，自动合并等在创建 cube 时填写的信息；
　4. `dict` 目录下有哪些 `database.table_name` 目录，取决于您在页面中加载了哪些表。`database.table_name` 目录下是使用 dict 作为编码方式的列名称目录。`column_name` 目录下存放的是 dict 文件，这些 dict 文件是与 HDFS 上 `fact_distinct_columns/table_name.column_name` 目录下的文件对应的，文件内容为该字段的基本信息；
　5. `execute` 目录下是构建 cube 时输出的构建信息包括每一步的参数信息及使用的类，对应使用的 HDFS 路径；
　6. `execute_output` 目录下是构建时的日志，包含报错信息；
　7. `model_desc` 目录下是 model_name.json 文件，内容主要为创建 model 时填写的信息，包括维度表，度量表，维度列，度量列以及分区列的描述信息；
　8. `project` 目录下是 project_name.json，文件内容为该 project 下的 table，cube 及 model 的名称；
　9. `table` 目录下是 table_name—project_name.json，文件内容为该表的信息，包括列名和数据类型等；
　10. `table_exd` 目录下是 table_name—project_name.json 文件，文件内容为数据源的属性；
　11. `user` 目录下有 ADMIN 文件，包含用户名，密码及权限等信息；
　12. `UUID` 文件是唯一标识符
* `sample_cube` 目录下有一个创建 sample 表（kylin_sales，kylin_account，kylin_cal_dt，kylin_category_groupings，kylin_country）的 sql 文件，`data` 目录和 `template` 目录。其中 `data` 目录下是这 5 张样例表的 csv 文件，包含有数据。`template` 目录下是样例项目的整套元数据。
* `spark` 目录下是 Kylin 自带的 spark
* `tomcat` 目录下是 Kylin 带的 tomcat，`temp` 目录下的 safeToDelete.tmp 是由 Tomcat 创建的，`kylin_job_metaXXX` 文件夹是由 FactDistinctColumnsJob，UHCDictionaryJob（当维度表中有高基维时触发），KafkaFlatTableJob，LookupTableToHFileJob 或 CubeHFileJob 构建出来的。
* 最好不要删除 `olap_model_XXX.json`，因为会在查询分析过程中被 Apache Calcite 使用。如果您不慎删除了，您可以通过重启 Kylin 的方式来创建它。
* `tool` 目录下是 `kylin-tool-<version>.jar`，jar 包中包含一些 Kylin 自带的类以及一些第三方类库。


### 检查运行环境

Kylin 运行在 Hadoop 集群上，对各个组件的版本、访问权限及 CLASSPATH 等都有一定的要求，为了避免遇到各种环境问题，您可以运行 `$KYLIN_HOME/bin/check-env.sh` 脚本来进行环境检测，如果您的环境存在任何的问题，脚本将打印出详细报错信息。如果没有报错信息，代表您的环境适合 Kylin 运行。



### 启动 Kylin

运行 `$KYLIN_HOME/bin/kylin.sh start` 脚本来启动 Kylin，界面输出如下：

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.5.0-bin-hbase1x
......
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-2.5.0-bin-hbase1x/logs/kylin.log
Web UI is at http://<hostname>:7070/kylin
```



### 使用 Kylin

Kylin 启动后您可以通过浏览器 `http://<hostname>:7070/kylin` 进行访问。
其中 `<hostname>` 为具体的机器名、IP 地址或域名，默认端口为 7070。
初始用户名和密码是 `ADMIN/KYLIN`。
服务器启动后，您可以通过查看 `$KYLIN_HOME/logs/kylin.log` 获得运行时日志。

### 生成的 HDFS 目录

在使用 Kylin 过程中，Kylin 将会创建以 `/hdfs-working-dir/metadata_name` 开头的 HDFS 文件夹，hdfs-working-dir 和 metadata_name 的名称可在 kylin.properties 中配置。`/hdfs-working-dir/metadata_name` 目录下有：`cardinality`，`coprocessor`，`kylin-job_id`，`resources` 这 4 个子目录。
1. `/hdfs-working-dir/metadata_name/cardinality`：`cardinality` 目录下是 `job_id` 目录。Job_id 可以从 Kylin 日志中找到。`job_id` 目录下有哪些 `database.table_name` 目录，取决于您在页面中加载了哪些表。`database.table_name` 目录下是 MR 任务运行后的结果文件，即计算出来的该表每一列的基数。文件一般很小，可以删除。
2. `/hdfs-working-dir/metadata_name/coprocessor`：`coprocessor` 目录下是对应的 coprocessor 的 jar 包，如果没有找到该目录，不必担心，在构建 cube 时会创建该 jar 包。
3. `/hdfs-working-dir/metadata_name/kylin-job_id`：`kylin-job_id` 目录下是 `cube_name` 的目录，`cube_name` 目录下分别有 `cuboid` 和 `rowkey_stats` 目录。
　　* `cuboid` 目录下是 `level-n-cuboid`（不同 level 的 cuboid）以及 `level_base_cuboid` 目录，再下一级就是构建 cuboid 的 MR 任务执行成功的结果文件，即 cuboid 数据，每行包含维度数组和 MeasureAggregator 数组，文件大小取决于每列的基数且通常来说都比较大。
　　* `rowkey_stats` 下是 part-r-00000_hfile 文件，在“Convert Cuboid Data to HFile”步骤时候使用，文件一般较小。
　　* 注意：`fact_distinct_columns`，`hfile`，`dictionary_shrunken` 这几个目录会在构建 cube 任务完成后被删除，因而如果你看到了这几个目录，你大可以放心删除。
　　* `fact_distinct_columns` 目录在 merge cube 时需要，该目录下有 `dict_info` 和 `statistics` 目录，是在“Merge Cuboid Dictionary”这步时作为输出的 dict 和 stat 路径，merge 操作完成后，会将合并的两个 segment 对应 job 的 HDFS 路径删掉，是从 `kylin-job_id` 这一级删除的。
　　* `hfile` 目录在“Convert Cuboid Data to HFile”这一步作为输出路径；而在“Load HFile to HBase”这一步会作为输入路径，其下一级的目录为列簇名。
4. `/hdfs-working-dir/metadata_name/resources`：`resources` 目录下又分别有 `cube_statistics`，`dict`, `table_snapshot`。
　　* 其中 `cube_statistics` 目录下是各个 `cube_name` 目录。而 `cube_name` 目录下存放的是该 cube 对应的 seg 文件。
　　* `dict` 目录下有哪些 `database.table_name` 目录，取决于您在页面中加载了哪些表。`database.table_name` 目录下是使用 dict 作为编码方式的列的名称目录。`column_name` 目录下存放的是 dict 文件，这些 dict 文件是与 HDFS 上 `fact_distinct_columns/table_name.column_name` 目录下的文件对应的，文件内容为该字段的基本信息。
　　* `table_snapshot` 目录下是 `database.table_name` 目录，该目录下存放的是这个表的 snapshot 文件。

### 停止 Kylin

运行 `$KYLIN_HOME/bin/kylin.sh stop` 脚本来停止 Kylin，界面输出如下：

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.5.0-bin-hbase1x
Stopping Kylin: 25964
Stopping in progress. Will check after 2 secs again...
Kylin with pid 25964 has been stopped.
```

您可以运行 `ps -ef | grep kylin` 来查看 Kylin 进程是否已停止。
