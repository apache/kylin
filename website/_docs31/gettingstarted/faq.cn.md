---
layout: docs31-cn
title:  常见问题
categories: 开始
permalink: /cn/docs31/gettingstarted/faq.html
since: v0.6.x
---

### 如果在使用 Kylin 中遇到了问题

1. 使用搜索引擎（谷歌/百度）、[Kylin 的邮件列表的存档](http://apache-kylin.74782.x6.nabble.com/), [Kylin 的 JIRA 列表](https://issues.apache.org/jira/projects/KYLIN/issues) 来寻求解决办法。
2. 浏览 Kylin 官方网页，尤其是[文档](http://kylin.apache.org/docs/) 和 [常见问题](http://kylin.apache.org/docs/gettingstarted/faq.html) 页面。
3. 向社区求助，用户可以在订阅 Apache Kylin 的邮件列表之后，用个人邮箱向 Apache Kylin 邮件列表发送邮件，所有订阅了邮件列表的用户都会看到此邮件，并回复邮件以发表自己的见解。
   Apache Kylin 主要有 3 个邮件列表，分别是 dev、user、issues。dev 列表主要讨论 Kylin 的开发及新版本发布，user 列表主要讨论用户使用过程中遇到的问题，issues 主要用于追踪 Kylin 项目管理工具（JIRA）的更新动态，订阅的方法请参考 [Apache Kylin 邮件群组](https://kylin.apache.org/cn/community/) 页面中的订阅方法。
   也正因为 Apache Kylin 社区是开源社区，所有用户和 Committer 都是志愿进行贡献的，所有的讨论和求助是没有 SLA（Service Level Agreement）的。为了提高讨论效率、规范提问，建议用户在撰写邮件时详细描述问题的出错情况、重现过程、安装版本和 Hadoop 发行版版本等，并且最好能提供相关的出错日志。另外，因为用户的全球化，建议提问时使用英文撰写邮件内容、至少保证邮件主题使用英文。有一篇关于如何提问的[How To Ask Questions The Smart Way](http://catb.org/~esr/faqs/smart-questions.html) 文章，推荐阅读。

### Kylin 是大数据的通用 SQL 引擎吗？
不，Kylin 是一个带有 SQL 接口的 OLAP 引擎。 SQL 查询需要与预定义的 OLAP 模型匹配。

### 什么是使用 Apache Kylin 的典型场景？
如果用户有一个巨大的表 (如：超过 1 亿行) 与维表进行 JOIN，而且查询需要在仪表盘、交互式报告、BI (商务智能) 中完成，用户并发数量为几十个或者几百个，那么 Kylin 是最好的选择。

### Kylin 支持多大的数据量表？ 性能怎么样？
Kylin 可以支持 TB 到 PB 级数据集的亚秒级查询。 这已经被 eBay，美团，头条等用户验证过。 以 美团的案例为例（至 2018-08），973 个 Cube，每天 380 万个查询，原始数据 8.9 万亿，总 Cube 大小 971 TB（原始数据更大），50％查询在 <0.5 秒内完成，90％ 查询 <1.2秒。

### Cube 的膨胀率是多大(与原始数据相比)
Cube 的膨胀率取决于多个因素，例如维度 / 度量的数量，维度的基数，Cuboid 的数量，压缩算法等。用户可以通过多种方式优化 Cube 体积。

### 如何比较 Kylin 与其他 SQL 引擎（如 Hive，Presto，SparkSQL，Impala）
SQL 引擎以不同的方式回答查询，Kylin 不是它们的替代品，而是它们的查询加速器。很多用户将 Kylin 与其他 SQL 引擎一起使用。对于高频率查询的模式，构建 Cube 可以极大地提高性能并给集群负荷减压。

### 运行 Kylin 需要多少个 Hadoop 节点？

Kylin 可以在 Hadoop 集群上运行，从几个节点到数千个节点，取决于您拥有多少数据。 该架构可水平扩展。
因为大多数计算都是在 Hadoop（MapReduce / Spark / HBase）中进行的，所以通常只需要在几个节点中安装Kylin。


### Cube 可以支持多少个维度？

Cube 的最大物理维度数量 (不包括衍生维度) 是 63，但是不推荐使用大于 30 个维度的 Cube，会引起维度灾难。


### 执行 "select \*" 报错
Cube 中只包含聚合数据，所以用户的所有查询都应该是聚合查询 (包含 "group by")。用户可以使用对所有维度分组（"group by") 的查询来获取尽可能接近的详细结果，但返回结果并不是原始数据。
为了从某些 BI 工具连接 Kylin，Kylin 会尝试回答 "select \*" 查询，但是请注意结果可能不是预期的。请确保发送到 Kylin 的每条查询都是聚合查询。


### 如何从 Cube 中查询原始数据

Cube 不是用于查询原始数据的正确选择；但是如果用户的确存在这个需求，以下是一些解决方案：
1. 将表中的主键（primiary key，简称 pk) 设置为 Cube 中的维度，然后在查询的时候使用 "group by pk"
2. 配置 Kylin 查询下压到其他 SQL 引擎如 Hive；但是请注意查询性能可能会有影响。


### 什么是超高基（UHC) 维度

UHC 代表 Ultra High Cardinality，即超高基数。基数表示维度不同值的数量。通常，维度的基数从数十到数百万。如果超过百万，我们将其称为超高基维度，例如：用户 ID，电话号码等。

Kylin 支持超高基维度，但是在 Cube 设计中额外注意超高基维度，它们可能会使 Cube 体积非常大、查询变慢。


### 如何指定用于回答查询的 Cube

用户无法指定用于回答查询的 Cube。Cube 对于终端用户来说是透明的，如果您对同一个数据模型有多个不同的 Cube，建议把不同的 Cube 放在不同的项目中。


### 存在用于创建项目、模型和 Cube 的 REST API 吗？

存在的，但是他们是代码内部的 API，并且会随着版本进行变化。默认情况下，Kylin 希望用户在网页上创建新的项目、模型和 Cube。

### 如何定义一个雪花模型 (有两张事实表)

在雪花模型中，同样只能指定一张事实表。但是用户可以定义一张维表与另一张维表进行 JOIN 连结。

如果查询中包含两张事实表的 JOIN 连结，用户同样可以将一张事实表定义为维表，并不为这张较大的维表设置维表快照。

### Cube 存放在哪里？可以直接在 HBase 上读取 Cube 数据吗？

Cube 数据存储在 HBase 中，每个 Cube segment 都是一张 HBase 中的表；维度的值将组成行键 (Row Key)；度量的值将按列序列化。为了提高存储效率，维度和度量都被编码为字节。Kylin 将在 HBase 中获取到字节后解码为原始值。
如果没有 Kylin 的元数据，HBase 表不是可读的。


### Cube 设计的最佳实践
请参考： [Design cube in Apache Kylin](https://www.slideshare.net/YangLi43/design-cube-in-apache-kylin)

### 如何加密 Cube 数据

用户可以在 HBase 端对 Cube 数据进行加密，有关内容请参考：[Transparent Encryption of Data At Rest](https://hbase.apache.org/book.html#hbase.encryption.server)


### 如何自动调度 Cube 构建

Kylin 没有内置的调度程度。您可以通过 REST API 从外部调度程度服务中触发 Cube 的定时构建，如 Linux 的命令 `crontab`、Apache Airflow 等。


### Kylin 支持 Hadoop 3 和 HBase 2.0 吗？

从 v2.5.0 开始，Kylin 将为 Hadoop 3 和 HBase 2 提供二进制包。


### Cube 已经启用了，但是 Insight 页面看不到对应的表

请确认 `$KYLIN_HOME/conf/kylin.properties` 中的配置项 `kylin.server.cluster-servers` 在每个 Kylin 节点上都被正确配置，Kylin 节点通过此配置相互通知刷新缓存，请确保节点间的网络通信健康。

### 如何处理 "java.lang.NoClassDefFoundError" 报错？

Kylin 并不自带这些 Hadoop 的 Jar 包，因为它们应该已经在 Hadoop 节点中存在。所以 Kylin 会尝试通过 `hbase classpath` 和 `hive -e set` 找到它们，并将它们的路径加入 `HBASE_CLASSPATH` 中（Kylin 启动时会运行 `hbase` 脚本，该脚本会读取 `HBASE_CLASSPATH`)。
由于 Hadoop 的复杂性，可能会存在一些找不到 Jar 包的情况，在这种情况下，请查看并修改 `$KYLIN_HOME/bin/` 目录下的 `find-\*-dependecy.sh` 和 `kylin.sh` 脚本来适应您的环境；或者在某些 Hadoop 的发行版中 (如 AWS EMR 5.0)，`hbase` 脚本不会保留原始的 `HBASE_CLASSPATH` 值，可能会引起 "NoClassDefFoundError" 的报错。为了解决这个问题，请在 `$HBASE_HOME/bin/` 下找到 `hbase` 脚本，并在其中搜索 `HBASE_CLASSPATH`，查看它是否是如下形式：
```sh
export HBASE_CLASSPATH=$HADOOP_CONF:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$ZOOKEEPER_HOME/*:$ZOOKEEPER_HOME/lib/*
```
如果是的话，请修改它来保留原始值，如下：
```sh
export HBASE_CLASSPATH=$HADOOP_CONF:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$ZOOKEEPER_HOME/*:$ZOOKEEPER_HOME/lib/*:$HBASE_CLASSPATH
```


### 如何在 Cube 中增加维度和度量？

一旦 Cube 被构建完成，它的结构不能被修改。用户可以通过克隆该 Cube，在新的 Cube 中增加维度和度量，并在新的 Cube 构建完成后，禁用或删除老的 Cube，使得查询能够使用新的 Cube 进行回答。

如果用户能接受新的维度在历史数据中不存在，则可以在老的 Cube 的构建结束时候后开始构建新的 Cube，并基于老的 Cube 和新的 Cube 创建一个混合（Hybrid）模型。


### 查询结果和在 Hive 中查询结果不一致

以下是可能的原因：
1. Hive 中的源数据在导入 Kylin 后发生了改变。
2. Cube 的时间区间和 Hive 中的不一样。
3. 另外一个 Cube 回答了查询。
4. 模型中存在内连接(INNER JOIN)，但是查询时没有包含所有的表的连接关系。
5. Cube 有一些近似度量，如 HyperLogLog、TopN
6. 在 Kylin v2.3 及之前版本，Kylin 在从 Hive 中获取数据时可能存在数据丢失，请参考 KYLIN-3388。

### Cube 构建完成后，源数据发生改变怎么办？

用户需要刷新 Cube，如果 Cube 被分区，那么可以刷新某些 Segment。

### 构建时在 “Load HFile to HBase Table” 报错 “bulk load aborted with some files not yet loaded”

根本原因是 Kylin 没有权限执行 HBase CompleteBulkLoad。请检查启动 Kylin 的用户是否有访问 HBase 的权限。

### 执行 `sample.sh` 在 HDFS 上创建 `/tmp/kylin` 文件夹失败

执行 `bash -v $KYLIN_HOME/bin/find-hadoop-conf-dir.sh`，查看报错信息，然后根据报错信息进行排错。

### 使用 Chrome 浏览器时，网页报错显示 “net::ERR_CONTENT_DECODING_FAILED”

修改 `$KYLIN_HOME/tomcat/conf/server.xml`，找到 `compress=on`，修改为 `off`。


### 如何配置某个 Cube 在指定的 YARN 队列中构建

用户可以在 Cube 级别进行重写如下参数：
```properties
kylin.engine.mr.config-override.mapreduce.job.queuename=YOUR_QUEUE_NAME
kylin.source.hive.config-override.mapreduce.job.queuename=YOUR_QUEUE_NAME
kylin.engine.spark-conf.spark.yarn.queue=YOUR_QUEUE_NAME
```

### 构建 Cube 时报错 “Too high cardinality is not suitable for dictionary”

Kylin 使用字典编码方式来编码/解码维度的值；通常一个维度的基数都是小于百万的，所以字典编码方式非常好用。正如字典需要被持久化、加载进内存，如果一个维度的基数非常高，内存占用会非常大，所以 Kylin 加了一层检查。如果用户看到这类报错，建议先找到哪些维度是超高基维度，并重新对 Cube 进行设计 (如：是否需要对将超高基的列设置为维度)，如果用户必须保留该超高基列作为一个维度，有如下解决方法：
1. 修改编码方式，如修改为 `fixed_length` 或 `integer`。
2. 增大 `$KYLIN_HOME/conf/kylin.properties` 中的 `kylin.dictionary.max.cardinality` 的值。


### 当某列中的数据都大于 0 时，查询 SUM() 返回负值

如果在 Hive 中将列声明为 integer，则 SQL 引擎 (Calcite) 将使用列的数据类型作为 “SUM()” 的数据类型，而此字段上的聚合值可能超出整数范围；在这种情况下，查询的返回结果可能是负值。

解决方法如下：
在 Hive 中将该列的数据类型更改为 BIGINT，然后将表同步到 Kylin（对应的 Cube 不需要刷新）。


### 为什么需要在构建 Cuboid 之前从事实表中提取不同的列？
Kylin 使用字典对每列中的值进行编码，这大大减少了 Cube 的存储大小。 而要构建字典，Kylin 需要为每列获取不同的值。


### 如何新增用户并修改默认密码

Kylin 的网页安全是通过 Spring 安全框架实现的，而 `kylinSecurity.xml` 是主要的配置文件。
```
${KYLIN_HOME}/tomcat/webapps/kylin/WEB-INF/classes/kylinSecurity.xml
```
预定义用户的密码哈希值可以在配置文件 “sandbox,testing” 中找到；如果要更改默认密码，用户需要生成一个新哈希，然后在此处更新，请参阅以下代码段： [Spring BCryptPasswordEncoder generate different password for same input](https://stackoverflow.com/questions/25844419/spring-bcryptpasswordencoder-generate-different-password-for-same-input)

我们更推荐集成 LDAP 认证方式和 Kylin 来管理多用户。

### 构建 Kylin 代码时遇到 NPM 报错 (中国大陆地区用户请特别注意此问题)

用户可以通过以下命令为 NPM 设置代理：
```sh
npm config set proxy http://YOUR_PROXY_IP
```
请更新您本地的 NPM 仓库以使用国内的 NPM 镜像，例如[淘宝 NPM 镜像]([http://npm.taobao.org](http://npm.taobao.org)


### 运行 BuildCubeWithEngineTest 失败 "failed to connect to hbase" 
用户在第一次运行 hbase 客户端时可能会遇到此错误，请检查错误信息以查看是否存在无法访问 "/hadoop/hbase/local/jars" 等文件夹；如果文件夹不存在，请创建它。

### JDBC 驱动程序返回的日期/时间与 REST API 不同
请参考：[JDBC query result Date column get wrong value](http://apache-kylin.74782.x6.nabble.com/JDBC-query-result-Date-column-get-wrong-value-td5370.html)


### 如何修改 ADMIN 用户的默认密码

默认情况下，Kylin 使用简单的、基于配置的用户注册表；默认的系统管理员 ADMIN 的密码为 KYLIN 在 `kylinSecurity.xml` 中进行了硬编码。如果要修改密码，首先需要获取新密码的加密值（使用BCrypt），然后在 `kylinSecurity.xml` 中设置它。以下为密码为 'ABCDE' 的示例：
```sh
cd $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib
java -classpath kylin-server-base-2.3.0.jar:spring-beans-4.3.10.RELEASE.jar:spring-core-4.3.10.RELEASE.jar:spring-security-core-4.2.3.RELEASE.jar:commons-codec-1.7.jar:commons-logging-1.1.3.jar org.apache.kylin.rest.security.PasswordPlaceholderConfigurer BCrypt ABCDE
```
加密后的密码为：
```
$2a$10$A7.J.GIEOQknHmJhEeXUdOnj2wrdG4jhopBgqShTgDkJDMoKxYHVu
```
然后将加密后的密码在 `kylinSecurity.xml` 中设置，如下：
```
vi $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/classes/kylinSecurity.xml
```
使用新的密码代替旧的密码：
```
<bean class="org.springframework.security.core.userdetails.User" id="adminUser">
	<constructor-arg value="ADMIN"/>
	<constructor-arg value="$2a$10$A7.J.GIEOQknHmJhEeXUdOnj2wrdG4jhopBgqShTgDkJDMoKxYHVu"/>
    <constructor-arg ref="adminAuthorities"/>
</bean>
```
重启 Kylin 来使得配置生效，如果用户有多个 Kylin 服务器作为一个集群，需要在所有的节点都执行相同操作。

### HDFS 上的工作目录中文件超过了 300G，可以手动删除吗？

HDFS 上的工作目录中的数据包括了中间数据 (将被垃圾清理所清除) 和 Cuboid 数据 (不会被垃圾清理所清除)，Cuboid 数据将为之后的 Segment 合并而保留。所以如果用户确认这些 Segment 在之后不会被合并，可以将 Cuboid 数据移动到其他路径甚至删除。

另外，请留意 HDFS 工作目录下的 "resources" 或 "jdbc-resources" 子目录中会存放一些大的元数据，如字典文件和维表的快照，这些文件不能被删除。

#### 如何对 like 语句中的关键字进行转义？
"%", "_" 是 "like" 语句中的保留关键字; "%" 可以匹配任意个字符,  "_" 匹配单个字符; 如果你想匹配关键字如 "_", 需要使用另一个字符在前面进行转义; 下面是一个使用 "/" 进行转义的例子, 此查询试图匹配 "xiao_":
"select username from gg_user where username like '%xiao/_%' escape '/'; "