---
layout: post-blog
title:  Improving Spark Cubing in Kylin 2.0
date:   2017-07-21 22:22:22
author: Kaisen Kang
categories: blog
---

Apache Kylin is a OALP Engine that speeding up query by Cube precomputation. The Cube is multi-dimensional dataset which contain precomputed all measures in all dimension combinations. Before v2.0, Kylin uses MapReduce to build Cube. In order to get better performance, Kylin 2.0 introduced the Spark Cubing. About the principle of Spark Cubing, please refer to the article [By-layer Spark Cubing][1].

In this blog, I will talk about the following topics:

 - How to make Spark Cubing support HBase cluster with Kerberos enabled 
 - Spark configurations for Cubing
 - Performance of Spark Cubing
 - Pros and cons of Spark Cubing
 - Applicable scenarios of Spark Cubing
 - Improvement for dictionary loading in Spark Cubing

In currently Spark Cubing(2.0) version, it doesn't support HBase cluster using Kerberos bacause Spark Cubing need to get matadata from HBase. To solve this problem, we have two solutions: one is to make Spark could connect HBase with Kerberos, the other is to avoid Spark connect to HBase in Spark Cubing.

### Make Spark connect HBase with Kerberos enabled
If just want to run Spark Cubing in Yarn client mode, we only need to add three line code before new SparkConf() in SparkCubingByLayer:

```
        Configuration configuration = HBaseConnection.getCurrentHBaseConfiguration();        
        HConnection connection = HConnectionManager.createConnection(configuration);
        //Obtain an authentication token for the given user and add it to the user's credentials.
        TokenUtil.obtainAndCacheToken(connection, UserProvider.instantiate(configuration).create(UserGroupInformation.getCurrentUser()));
```

As for How to make Spark connect HBase using Kerberos in Yarn cluster mode, please refer to SPARK-6918, SPARK-12279, and HBASE-17040. The solution may work, but not elegant. So I tried the sencond solution.

### Use HDFS metastore for Spark Cubing

The core idea here is uploading the necessary metadata job related to HDFS and using HDFSResourceStore manage the metadata.

Before introducing how to use HDFSResourceStore instead of HBaseResourceStore in Spark Cubing. Let’s see what's Kylin metadata format and how Kylin manages the metadata.

Every concrete metadata for table, cube, model and project is a JSON file in Kylin. The whole metadata is organized by file directory. The picture below is the root directory for Kylin metadata,
![屏幕快照 2017-07-02 下午3.51.43.png-20.7kB][2]
This following picture shows the content of project dir, the "learn_kylin" and "kylin_test" are both project names.
![屏幕快照 2017-07-02 下午3.54.59.png-11.8kB][3]

Kylin manage the metadata using ResourceStore, ResourceStore is a abstract class, which abstract the CRUD Interface for metadata. ResourceStore has three implementation classes：

 - FileResourceStore  (store with Local FileSystem)
 - HDFSResourceStore  
 - HBaseResourceStore         

Currently, only HBaseResourceStore could use in production env. FileResourceStore mainly used for testing. HDFSResourceStore doesn't support massive concurrent write, but it is ideal to use for read only scenario like Cubing. Kylin use the "kylin.metadata.url" config to decide which kind of ResourceStore will be used.

Now, Let’s see How to use HDFSResourceStore instead of HBaseResourceStore in Spark Cubing.

 1. Determine the necessary metadata for Spark Cubing job
 2. Dump the necessary metadata from HBase to local
 3. Update the kylin.metadata.url and then write all Kylin config to "kylin.properties" file in local metadata dir.
 4. Use ResourceTool upload the local metadata to HDFS.
 5. Construct the HDFSResourceStore from the HDFS "kylin.properties" file in Spark executor.

Of course, We need to delete the HDFS metadata dir on complete. I'm working on a patch for this, please watch KYLIN-2653 for update.
  
### Spark configurations for Cubing

Following is the Spark configuration I used in our environment. It enables Spark dynamic resource allocation; the goal is to let our user set less Spark configurations.

```
//running in yarn-cluster mode
kylin.engine.spark-conf.spark.master=yarn
kylin.engine.spark-conf.spark.submit.deployMode=cluster 

//enable the dynamic allocation for Spark to avoid user set the number of executors explicitly
kylin.engine.spark-conf.spark.dynamicAllocation.enabled=true
kylin.engine.spark-conf.spark.dynamicAllocation.minExecutors=10
kylin.engine.spark-conf.spark.dynamicAllocation.maxExecutors=1024
kylin.engine.spark-conf.spark.dynamicAllocation.executorIdleTimeout=300
kylin.engine.spark-conf.spark.shuffle.service.enabled=true
kylin.engine.spark-conf.spark.shuffle.service.port=7337

//the memory config
kylin.engine.spark-conf.spark.driver.memory=4G
//should enlarge the executor.memory when the cube dict is huge
kylin.engine.spark-conf.spark.executor.memory=4G 
//because kylin need to load the cube dict in executor
kylin.engine.spark-conf.spark.executor.cores=1

//enlarge the timeout
kylin.engine.spark-conf.spark.network.timeout=600

kylin.engine.spark-conf.spark.yarn.queue=root.hadoop.test

kylin.engine.spark.rdd-partition-cut-mb=100
```

### Performance test of Spark Cubing

For the source data scale from millions to hundreds of millions, my test result is consistent with the blog [By-layer Spark Cubing][1]. The improvement is remarkable. Moreover, I also tested with billions of source data and having huge dictionary specially.

The test Cube1 has 2.7 billion source data, 9 dimensions, one precise distinct count measure having 70 million cardinality (which means the dict also has 70 million cardinality).

Test test Cube2 has 2.4 billion source data, 13 dimensions, 38 measures(contains 9 precise distinct count measures).

The test result is shown in below picture, the unit of time is minute.
![image.png-38.1kB][4]

In one word, **Spark Cubing is much faster than MR cubing in most scenes**.

### Pros and Cons of Spark Cubing
In my opinion, the advantage for Spark Cubing includes:

 1. Because of the RDD cache, Spark Cubing could take full advantage of memory to avoid disk I/O.
 2. When we have enough memory resource, Spark Cubing could use more memory resource to get better build performance.

On the contrary，the drawback for Spark Cubing includes:

 1. Spark Cubing couldn't handle huge dictionary well (hundreds of millions of cardinality);
 2. Spark Cubing isn't stable enough for very large scale data.

### Applicable scenarios of Spark Cubing
In my opinion, except the huge dictionary scenario, we all could use Spark Cubing to replace MR Cubing, especially under the following scenarios:

 1. Many dimensions
 2. Normal dictionaries (e.g, cardinality < 1 hundred millions)
 3. Normal scale data (e.g, less than 10 billion rows to build at once).

### Improvement for dictionary loading in Spark Cubing

As we all known, a big difference for MR and Spark is, the task for MR is running in process, but the task for Spark is running in thread. So, in MR Cubing, the dict of Cube only load once, but in Spark Cubing, the dict will be loaded many times in one executor, which will cause frequent GC.

So, I made the two improvements:

 1. Only load the dict once in one executor.
 2. Add maximumSize for LoadingCache in the AppendTrieDictionary to make the dict removed as early as possible.

 These two improvements have been contributed into Kylin repository.

### Summary
Spark Cubing is a great feature for Kylin 2.0, Thanks Kylin community. We will apply Spark Cubing in real scenarios in our company. I believe Spark Cubing will be more robust and efficient in the future releases.
 
 
  [1]: http://kylin.apache.org/blog/2017/02/23/by-layer-spark-cubing/
  [2]: http://static.zybuluo.com/kangkaisen/t1tc6neiaebiyfoir4fdhs11/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-07-02%20%E4%B8%8B%E5%8D%883.51.43.png
  [3]: http://static.zybuluo.com/kangkaisen/4dtiioqnw08w6vtj0r9u5f27/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-07-02%20%E4%B8%8B%E5%8D%883.54.59.png
  [4]: http://static.zybuluo.com/kangkaisen/1urzfkal8od52fodi1l6u0y5/image.png