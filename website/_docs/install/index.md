---
layout: docs
title:  "Installation Guide"
categories: install
permalink: /docs/install/index.html
---

### Software Requirements

* Hadoop: 2.7+, 3.1+ (since v2.5)
* Hive: 0.13 - 1.2.1+
* HBase: 1.1+, 2.0 (since v2.5)
* Spark (optional) 2.1.1+
* Kafka (optional) 1.0.0+ (since v2.5)
* JDK: 1.8+ (since v2.5)
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

Tests passed on Hortonworks HDP 2.2-2.6 and 3.0, Cloudera CDH 5.7-5.11 and 6.0, AWS EMR 5.7-5.10, Azure HDInsight 3.5-3.6.

We recommend you to try out Kylin or develop it using the integrated sandbox, such as [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/), and make sure it has at least 10 GB of memory. When configuring a sandbox, we recommend that you use the Bridged Adapter model instead of the NAT model.



### Hardware Requirements

The minimum configuration of a server running Kylin is 4 core CPU, 16 GB RAM and 100 GB disk. For high-load scenarios, a 24-core CPU, 64 GB RAM or higher is recommended.



### Hadoop Environment

Kylin relies on Hadoop clusters to handle large data sets. You need to prepare a Hadoop cluster with HDFS, YARN, MapReduce, Hive, HBase, Zookeeper and other services for Kylin to run.
Kylin can be launched on any node in a Hadoop cluster. For convenience, you can run Kylin on the master node. For better stability, it is recommended to deploy Kylin on a clean Hadoop client node with Hive, HBase, HDFS and other command lines installed and client configuration (such as `core-site.xml`, `hive-site.xml`, `hbase-site.xml` and others) are also reasonably configured and can be automatically synchronized with other nodes.

Linux accounts running Kylin must have access to the Hadoop cluster, including the permission to create/write HDFS folders, Hive tables, HBase tables, and submit MapReduce tasks.



### Kylin Installation

1. Download a binary package for your Hadoop version from the [Apache Kylin Download Site](https://kylin.apache.org/download/). For example, Kylin 2.5.0 for HBase 1.x can be downloaded from the following command line:

```shell
Cd /usr/local/
Wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-2.5.0/apache-kylin-2.5.0-bin-hbase1x.tar.gz
```

2. Unzip the tarball and configure the environment variable `$KYLIN_HOME` to the Kylin folder.

```shell
Tar -zxvf apache-kylin-2.5.0-bin-hbase1x.tar.gz
Cd apache-kylin-2.5.0-bin-hbase1x
Export KYLIN_HOME=`pwd`
```

### Unzipped Directory of Kylin Tar Package
* `bin` directory mainly contains some shell scripts, including checking ports, hive availability files and the files used to search the dependencies of HBase, Hive, Kafka, Spark.
* `conf` directory mainly contains some xml configuration files. The function of these xml documents can be seen in [this page](http://kylin.apache.org/docs/install/configuration.html)
* `lib` directory mainly contains some jar packages.
* `meta_backups` directory will appear after the metadata is backed up. Its subdirectory will be divided into different directories according to different backup times, named `meta_year_month_day_hour_minute_second`. Under this directory, there are directories and files such as `acl`, `cube`, `cube_desc`, `dict`, `execute`, `execute_output`, `model_desc`, `project`, `table`, `table_exd`, `user`, `UUID`. 
　1. There is a file under `acl` directory, which contains uuid, last_modified, version, domainObjectInfo(type, id), parentDomainObjectInfo, ownerInfo(sid, principal), entriesInheriting and entries（p,m）information;
　2. What is stored under the `cube` is the cube building information under the metadata, which corresponds to cube_name.json, including segment information, cube status and other information; 
　3. The information created by the cube under the metadata is stored under `cube_desc`, and different cubes correspond to different cube_name.json, including the information filled in when creating the cube, such as dimensions, measures, dictionaries, rowkey aggregation groups, partition start time, automatic merging, etc; 
　4. Below `dict` is `database.table_name` directory (the name of this directory depends on the tables you load in the page). Under `database.table_name` directory (the name of this directory is the name of the column that uses dict as the encoding method) is `column_name` directory. Under `column_name` directory is `.dict` file that corresponds to the file under `fact_distinct_columns/table_name.column_name` directory on HDFS, which contains the basic information of this field; 
　5. Under `execute` directory, the construction information output during cube building includes the parameter information of each step, the class used, and the corresponding HDFS path used; 
　6. Under `execute_output` directory is the log at the time of construction, containing error reporting information; 
　7. Under `model_desc` directory is the `model_name.json` file, which mainly contains information filled in when creating the model, including description information of dimension tables, measurement tables, dimension columns, measurement columns and partition columns; 
　8. Under `project` directory is project_name.json that contains the names of tables, cubes and models; 
　9. Under the `table` directory is table_name-project_name.json that contains the information of the table, including column name data type, etc; 
　10. Under `table_exd` directory is table_name-project_name.json file that contains the attributes of the data source; 
　11. The `user` directory has ADMIN file, which contains information such as username, password and permissions; 
　12. `UUID` file is a unique identifier.
* There is an SQL file under `sample_cube` directory to create the sample tables (kylin_sales, kylin_account, kylin_cal_dt, kylin_category_groupings, kylin_country), `data` directory and `template` directory. The csv files including data of the five sample tables are located under the `data` directory. And the `template` directory contains the entire set of metadata for the sample project.
* Spark under `spark` directory is built by Kylin.
* Tomcat under `tomcat` directory is built by Kylin. `safeToDelete.tmp` under `temp` directory is created by Tomcat. `kylin_job_metaXXX` folder is created by FactDistinctColumnsJob, UHCDictionaryJob (triggered when there is a high cardinality dimension in the dimension table), KafkaFlatTableJob, LookupTableToHFileJob or CubeHFileJob. 
* You'd better not delete `olap_model_XXX.json` because it will be used by Apache Calcite during query analysis. If you delete it carelessly, you can create it by restarting Kylin. 
* Under `tool` directory is `kylin-tool-<version>.jar`, the jar package containing some of Kylin's own classes and some third-party class libraries. 

### Checking the operating environment

Kylin runs on a Hadoop cluster and has certain requirements for the version, access rights, and CLASSPATH of each component. To avoid various environmental problems, you can run the script, `$KYLIN_HOME/bin/check-env.sh` to have a test on your environment, if there are any problems with your environment, the script will print a detailed error message. If there is no error message, it means that your environment is suitable for Kylin to run.



### Start Kylin

Run the script, `$KYLIN_HOME/bin/kylin.sh start` , to start Kylin. The interface output is as follows:

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.5.0-bin-hbase1x
......
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-2.5.0-bin-hbase1x/logs/kylin.log
Web UI is at http://<hostname>:7070/kylin
```



### Using Kylin

Once Kylin is launched, you can access it via the browser `http://<hostname>:7070/kylin` with
specifying `<hostname>` with IP address or domain name, and the default port is 7070.
The initial username and password are `ADMIN/KYLIN`.
After the server is started, you can view the runtime log, `$KYLIN_HOME/logs/kylin.log`.

### Generated HDFS Directory
There are four directories under `/hdfs-working-dir/metadata_name`: `cardinality`, `coprocessor`, `kylin-job_id`, `resources`. 
1. `/hdfs-working-dir/metadata_name/cardinality`: under `cardinality` directory is `job_id` directory. Job_id can be found in Kylin log. Under `job_id` directory is `database.table_name` directory (the name of this directory depends on the tables you load in the page). The files under `database.table_name` directory are the results of the MR tasks, i.e. the calculated cardinality of each column in the table. The files are generally small and can be deleted. 
2. `/hdfs-working-dir/metadata_name/coprocessor`: under the directory of `coprocessor` is the jar package of the corresponding coprocessor. If you can not find the directory, don't worry. The jar package will be created when the cube is built. 
3. `/hdfs-working-dir/metadata_name/kylin-job_id`: under `kylin-job_id directory` is the directory of `cube_name`. `cuboid` and `rowkey_stats` directories are under `cube_name` directory. 
　　* Under `cuboid` directory are the `level-n-cuboid` (cuboid of different levels) and `level_base_cuboid` directory, their subdirectories are the results of the MR tasks to build cuboid, namely cuboid data. Each row contains a dimensioned array and a MeasureAggregator array. The file size depends on the cardinality of each column and is usually relatively large. 
　　* Under `rowkey_stats` is a part-r-00000_hfile file, which is generally small and used in the "Convert Cuboid Data to HFile" step. 
　　* Note: the `fact_distinct_columns`, `hfile`, `dictionary_shrunken` directories will be deleted after the cube building task is completed. Therefore, if you see these directories, you can safely delete them. 
　　* The `fact_distinct_columns` directory is required when merging cubes. There are `dict_info` and `statistics` directories under this directory, which are the dict and stat paths used as outputs during the "Merge Cuboid Dictionary" step. After the operation of merging cubes is completed, the HDFS path of the job corresponding to the merged two segments will be deleted, which is deleted from the `kylin-job_id` level. 
　　* The `hfile` directory is used in the step of "Convert Cuboid Data to HFile" as the output path. In the step of "Load HFile to HBase", it will be used as the input path, and its subdirectory is the column family name. 
4. `/hdfs-working-dir/metadata_name/resources`: There are `cube_statistics`, `dict`, `table_snapshot` under the `resources` directory. 
　　* The `cube_statistics` directory contains all `cube_name` directories. And the seg files corresponding to the cube are stored in `cube_name` directory. 
　　* Below `dict` is `database.table_name` directory. The name of `database.table_name` directory depends on the tables you load in the page. And under the `database.table_name` directory is `column_name` directory. The name of `column_name` directory is the name of the column that uses dict as the encoding method. Under `column_name` directory is `.dict` file that corresponds to the file under `fact_distinct_columns/table_name.column_name` directory on HDFS, which contains the basic information of this field. 
　　* Under `table_snapshot` directory is `database.table_name` directory. The snapshot files of the table are stored under `database.table_name` directory. 


### Stop Kylin

Run the `$KYLIN_HOME/bin/kylin.sh stop` script to stop Kylin. The console output is as follows:

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.5.0-bin-hbase1x
Stopping Kylin: 25964
Stopping in progress. Will check after 2 secs again...
Kylin with pid 25964 has been stopped.
```

You can run `ps -ef | grep kylin` to see if the Kylin process has stopped.