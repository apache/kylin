---
layout: docs31-cn
title:  优化 Cube 构建
categories: 帮助
permalink: /cn/docs31/howto/howto_optimize_build.html
---

Kylin将Cube构建任务分解为几个依次执行的步骤，这些步骤包括Hive操作、MapReduce操作和其他类型的操作。如果你有很多Cube构建任务需要每天运行，那么你肯定想要减少其中消耗的时间。下文按照Cube构建步骤顺序提供了一些优化经验。

## 创建Hive的中间平表

这一步将数据从源Hive表提取出来(和所有join的表一起)并插入到一个中间平表。如果Cube是分区的，Kylin会加上一个时间条件以确保只有在时间范围内的数据才会被提取。你可以在这个步骤的log查看相关的Hive命令，比如：

```
hive -e "USE default;
DROP TABLE IF EXISTS kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34;

CREATE EXTERNAL TABLE IF NOT EXISTS kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34
(AIRLINE_FLIGHTDATE date,AIRLINE_YEAR int,AIRLINE_QUARTER int,...,AIRLINE_ARRDELAYMINUTES int)
STORED AS SEQUENCEFILE
LOCATION 'hdfs:///kylin/kylin200instance/kylin-0a8d71e8-df77-495f-b501-03c06f785b6c/kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34';

SET dfs.replication=2;
SET hive.exec.compress.output=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=100000000;
SET mapreduce.job.split.metainfo.maxsize=-1;

INSERT OVERWRITE TABLE kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34 SELECT
AIRLINE.FLIGHTDATE
,AIRLINE.YEAR
,AIRLINE.QUARTER
,...
,AIRLINE.ARRDELAYMINUTES
FROM AIRLINE.AIRLINE as AIRLINE
WHERE (AIRLINE.FLIGHTDATE >= '1987-10-01' AND AIRLINE.FLIGHTDATE < '2017-01-01');

```

在Hive命令运行时，Kylin会用`conf/kylin_hive_conf.properties`里的配置，比如保留更少的冗余备份和启用Hive的mapper side join。需要的话可以根据集群的具体情况增加其他配置。

如果cube的分区列(在这个案例中是"FIGHTDATE")与Hive表的分区列相同，那么根据它过滤数据能让Hive聪明地跳过不匹配的分区。因此强烈建议用Hive的分区列（如果它是日期列）作为cube的分区列。这对于那些数据量很大的表来说几乎是必须的，否则Hive不得不每次在这步扫描全部文件，消耗非常长的时间。

如果启用了Hive的文件合并，你可以在`conf/kylin_hive_conf.xml`里关闭它，因为Kylin有自己合并文件的方法(下一节)：

    <property>
        <name>hive.merge.mapfiles</name>
        <value>false</value>
        <description>Disable Hive's auto merge</description>
    </property>

## 重新分发中间表

在之前的一步之后，Hive在HDFS上的目录里生成了数据文件：有些是大文件，有些是小文件甚至空文件。这种不平衡的文件分布会导致之后的MR任务出现数据倾斜的问题：有些mapper完成得很快，但其他的就很慢。针对这个问题，Kylin增加了这一个步骤来“重新分发”数据，这是示例输出:

```
total input rows = 159869711
expected input rows per mapper = 1000000
num reducers for RedistributeFlatHiveTableStep = 160

```

重新分发表的命令：

```
hive -e "USE default;
SET dfs.replication=2;
SET hive.exec.compress.output=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=100000000;
SET mapreduce.job.split.metainfo.maxsize=-1;
set mapreduce.job.reduces=160;
set hive.merge.mapredfiles=false;

INSERT OVERWRITE TABLE kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34 SELECT * FROM kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34 DISTRIBUTE BY RAND();
"
```

首先，Kylin计算出中间表的行数，然后基于行数的大小算出重新分发数据需要的文件数。默认情况下,Kylin为每一百万行分配一个文件。在这个例子中，有1.6亿行和160个reducer，每个reducer会写一个文件。在接下来对这张表进行的MR步骤里，Hadoop会启动和文件相同数量的mapper来处理数据(通常一百万行数据比一个HDFS数据块要小)。如果你的日常数据量没有这么大或者Hadoop集群有足够的资源，你或许想要更多的并发数，这时可以将`conf/kylin.properties`里的`kylin.job.mapreduce.mapper.input.rows`设为小一点的数值，比如:

`kylin.job.mapreduce.mapper.input.rows=500000`

其次，Kylin会运行 *"INSERT OVERWRITE TABLE ... DISTRIBUTE BY "* 形式的HiveQL来分发数据到指定数量的reducer上。

在很多情况下，Kylin请求Hive随机分发数据到reducer，然后得到大小相近的文件，分发的语句是"DISTRIBUTE BY RAND()"。

如果你的cube指定了一个高基数的列，比如"USER_ID"，作为"分片"维度(在cube的“高级设置”页面)，Kylin会让Hive根据该列的值重新分发数据，那么在该列有着相同值的行将被分发到同一个文件。这比随机要分发要好得多，因为不仅重新分布了数据，并且在没有额外代价的情况下对数据进行了预先分类，如此一来接下来的cube build处理会从中受益。在典型的场景下，这样优化可以减少40%的build时长。在这个案例中分发的语句是"DISTRIBUTE BY USER_ID"：

请注意: 1)“分片”列应该是高基数的维度列，并且它会出现在很多的cuboid中（不只是出现在少数的cuboid）。 使用它来合理进行分发可以在每个时间范围内的数据均匀分布，否则会造成数据倾斜，从而降低build效率。典型的正面例子是：“USER_ID”、“SELLER_ID”、“PRODUCT”、“CELL_NUMBER”等等，这些列的基数应该大于一千(远大于reducer的数量)。 2)"分片"对cube的存储同样有好处，不过这超出了本文的范围。

## 提取事实表的唯一列

在这一步骤Kylin运行MR任务来提取使用字典编码的维度列的唯一值。

实际上这步另外还做了一些事情：通过HyperLogLog计数器收集cube的统计数据，用于估算每个cuboid的行数。如果你发现mapper运行得很慢，这通常表明cube的设计太过复杂，请参考
[优化cube设计](howto_optimize_cubes.html)来简化cube。如果reducer出现了内存溢出错误，这表明cuboid组合真的太多了或者是YARN的内存分配满足不了需要。如果这一步从任何意义上讲不能在合理的时间内完成，你可以放弃任务并考虑重新设计cube，因为继续下去会花费更长的时间。

你可以通过降低取样的比例（kylin.job.cubing.inmen.sampling.percent）来加速这个步骤，但是帮助可能不大而且影响了cube统计数据的准确性，所有我们并不推荐。

## 构建维度字典

有了前一步提取的维度列唯一值，Kylin会在内存里构建字典。通常这一步比较快，但如果唯一值集合很大，Kylin可能会报出类似“字典不支持过高基数”。对于UHC类型的列，请使用其他编码方式，比如“fixed_length”、“integer”等等。

## 保存cuboid的统计数据和创建 HTable

这两步是轻量级和快速的。

## 构建基础cuboid

这一步用Hive的中间表构建基础的cuboid，是“逐层”构建cube算法的第一轮MR计算。Mapper的数目与第二步的reducer数目相等；Reducer的数目是根据cube统计数据估算的：默认情况下每500MB输出使用一个reducer；如果观察到reducer的数量较少，你可以将kylin.properties里的“kylin.job.mapreduce.default.reduce.input.mb”设为小一点的数值以获得过多的资源，比如:

`kylin.job.mapreduce.default.reduce.input.mb=200`

## Build N-Dimension Cuboid 
## 构建N维cuboid

这些步骤是“逐层”构建cube的过程，每一步以前一步的输出作为输入，然后去掉一个维度以聚合得到一个子cuboid。举个例子，cuboid ABCD去掉A得到BCD，去掉B得到ACD。

有些cuboid可以从一个以上的父cuboid聚合得到，这种情况下，Kylin会选择最小的一个父cuboid。举例,AB可以从ABC(id:1110)和ABD(id:1101)生成，则ABD会被选中，因为它的比ABC要小。在这基础上，如果D的基数较小，聚合运算的成本就会比较低。所以，当设计rowkey序列的时候，请记得将基数较小的维度放在末尾。这样不仅有利于cube构建，而且有助于cube查询，因为预聚合也遵循相同的规则。

通常来说，从N维到(N/2)维的构建比较慢，因为这是cuboid数量爆炸性增长的阶段：N维有1个cuboid，(N-1)维有N个cuboid，(N-2)维有N*(N-1)个cuboid，以此类推。经过(N/2)维构建的步骤，整个构建任务会逐渐变快。

## 构建cube

这个步骤使用一个新的算法来构建cube：“逐片”构建（也称为“内存”构建）。它会使用一轮MR来计算所有的cuboids，但是比通常情况下更耗内存。配置文件"conf/kylin_job_inmem.xml"正是为这步而设。默认情况下它为每个mapper申请3GB内存。如果你的集群有充足的内存，你可以在上述配置文件中分配更多内存给mapper，这样它会用尽可能多的内存来缓存数据以获得更好的性能，比如：

    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>6144</value>
        <description></description>
    </property>
    
    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx5632m</value>
        <description></description>
    </property>


请注意，Kylin会根据数据分布（从cube的统计数据里获得）自动选择最优的算法，没有被选中的算法对应的步骤会被跳过。你不需要显式地选择构建算法。

## 将cuboid数据转换为HFile

这一步启动一个MR任务来讲cuboid文件（序列文件格式）转换为HBase的HFile格式。Kylin通过cube统计数据计算HBase的region数目，默认情况下每5GB数据对应一个region。Region越多，MR使用的reducer也会越多。如果你观察到reducer数目较小且性能较差，你可以将“conf/kylin.properties”里的以下参数设小一点，比如：

```
kylin.hbase.region.cut=2
kylin.hbase.hfile.size.gb=1
```

如果你不确定一个region应该是多大时，联系你的HBase管理员。

## 将HFile导入HBase表

这一步使用HBase API来讲HFile导入region server，这是轻量级并快速的一步。

## 更新cube信息

在导入数据到HBase后，Kylin在元数据中将对应的cube segment标记为ready。

## 清理资源

将中间宽表从Hive删除。这一步不会阻塞任何操作，因为在前一步segment已经被标记为ready。如果这一步发生错误，不用担心，垃圾回收工作可以晚些再通过Kylin的[StorageCleanupJob](howto_cleanup_storage.html)完成。

## 总结
还有非常多其他提高Kylin性能的方法，如果你有经验可以分享，欢迎通过[dev@kylin.apache.org](mailto:dev@kylin.apache.org)讨论。
