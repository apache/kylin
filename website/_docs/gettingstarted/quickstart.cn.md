---
layout: docs-cn
title:  快速开始
categories: 开始
permalink: /cn/docs/gettingstarted/kylin-quickstart.html
since: v0.6.x
---

这里是一份从下载安装到体验亚秒级查询的完整流程，你可以按照文章里的步骤对kylin进行初步的了解和体验，掌握kylin的基本使用技能，然后结合自己的业务场景使用kylin来设计模型，加速查询。

### 基于hadoop环境安装使用kylin

对于已经有稳定hadoop环境的用户，可以下载kylin的二进制包将其部署安装在自己的hadoop集群。安装之前请根据以下要求进行环境检查：

- 前置条件：
Kylin 依赖于 Hadoop 集群处理大量的数据集。您需要准备一个配置好 HDFS，YARN，MapReduce，Hive，Zookeeper 和其他服务的 Hadoop 集群供 Kylin 运行。
Kylin 可以在 Hadoop 集群的任意节点上启动。方便起见，您可以在 master 节点上运行 Kylin。但为了更好的稳定性，我们建议您将 Kylin 部署在一个干净的 Hadoop client 节点上，该节点上 Hive，HDFS 等命令行已安装好且 client 配置（如 core-site.xml，hive-site.xml及其他）也已经合理的配置且其可以自动和其它节点同步。
运行 Kylin 的 Linux 账户要有访问 Hadoop 集群的权限，包括创建/写入 HDFS 文件夹，Hive 表的权限。

- 硬件要求：
运行 Kylin 的服务器建议最低配置为 4 core CPU，16 GB 内存和 100 GB 磁盘。

- 操作系统要求：
CentOS 6.5+ 或Ubuntu 16.0.4+

- 软件要求：
  - Hadoop 2.7+,3.0
  - Hive 0.13+,1.2.1+
  - Spark 2.4.6
  - JDK: 1.8+

建议使用集成的Hadoop环境进行kylin的安装与测试，比如Hortonworks HDP 或Cloudera CDH ，kylin发布前在 Hortonworks HDP 2.4, Cloudera CDH 5.7 and 6.0, AWS EMR 5.31 and 6.0, Azure HDInsight 4.0 上测试通过。 

当你的环境满足上述前置条件时 ，你可以开始安装使用kylin。

#### step1、下载kylin压缩包

从[Apache Kylin Download Site](https://kylin.apache.org/download/)下载 kylin4.0 的二进制文件。
```
cd /usr/local/
wget http://apache.website-solution.net/kylin/apache-kylin-4.0.0/apache-kylin-4.0.0-bin.tar.gz
```

#### step2、解压kylin

解压下载得到的kylin压缩包，并配置环境变量KYLIN_HOME指向解压目录：

```
tar -zxvf  apache-kylin-4.0.0-bin.tar.gz
cd apache-kylin-4.0.0-bin-cdh57
export KYLIN_HOME=`pwd`
```

#### step3、下载SPARK

Kylin4.0 使用 Spark 作为查询和构建引擎，所以你需要设置SPARK_HOME指向自己的spark安装路径：

```
export SPARK_HOME=/path/to/spark
```

如果您没有已经下载好的Spark环境，也可以使用kylin自带脚本下载spark:

```
$KYLIN_HOME/bin/download-spark.sh
```

脚本会将解压好的spark放在$KYLIN_HOME目录下，如果系统中没有设置SPARK_HOME，启动kylin时会自动找到$KYLIN_HOME目录下的spark。

### step4、配置 Mysql 元数据

Kylin 4.0 使用 Mysql 作为元数据存储，需要在kylin.properties做如下配置：

```$xslt
kylin.metadata.url=kylin_metadata@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://localhost:3306/kylin_database,username=,password=
kylin.env.zookeeper-connect-string=ip:2181
```

你需要修改其中的 Mysql 用户名和密码，以及存储元数据的database和table。请参考 [配置 Mysql 为 Metastore](/cn/docs/tutorial/mysql_metastore.html)  了解 Mysql 作为 Metastore 的详细配置。

#### step5、环境检查

Kylin 运行在 Hadoop 集群上，对各个组件的版本、访问权限及 CLASSPATH 等都有一定的要求，为了避免遇到各种环境问题，您可以执行

```
$KYLIN_HOME/bin/check-env.sh
```

来进行环境检测，如果您的环境存在任何的问题，脚本将打印出详细报错信息。如果没有报错信息，代表您的环境适合 Kylin 运行。

#### step6、启动kylin

运行如下命令来启动kylin：

```
$KYLIN_HOME/bin/kylin.sh start 
```

如果启动成功，命令行的末尾会输出如下内容：

```
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-4.0.0-bin/logs/kylin.log
Web UI is at http://<hostname>:7070/kylin
```

#### step7、访问kylin

Kylin 启动后您可以通过浏览器 http://<hostname>:port/kylin 进行访问。
其中 <hostname> 为具体的机器名、IP 地址或域名，port为kylin端口，默认为7070。
初始用户名和密码是 ADMIN/KYLIN。
服务器启动后，可以通过查看 $KYLIN_HOME/logs/kylin.log 获得运行时日志。

#### step8、创建Sample Cube

Kylin提供了一个创建样例Cube的脚本，以供用户快速体验Kylin。
在命令行运行：

```
$KYLIN_HOME/bin/sample.sh
```

完成后登陆kylin，点击System->Configuration->Reload Metadata来重载元数据
元数据重载完成后你可以在左上角的Project中看到一个名为learn_kylin的项目，它包含kylin_sales_cube和kylin_streaming_cube, 它们分别为batch cube和streaming cube，不过 kylin4.0 暂时还不支持 streaming cube，你可以直接对kylin_sales_cube进行构建，构建完成后就可以查询。

当然，你也可以根据下面的教程来尝试创建自己的Cube。

#### step9、创建project

登陆kylin后，点击左上角的+号来创建Project：

![](/images/docs/quickstart/create_project.png)

#### step10、加载Hive表

点击Model->Data Source->Load Table From Tree，
Kylin会读取到Hive数据源中的表并以树状方式显示出来，你可以选择自己要使用的表，然后点击sync进行将其加载到kylin。

此外，Kylin4.0 还支持 CSV 格式文件作为数据源，你也可以点击 Model->Data Source->Load CSV File as Table 来加载 CSV 数据源。

本例中仍然使用 Hive 数据源进行讲解与演示。 

![](/images/docs/quickstart/load_hive_table.png)

#### step11、创建模型

点击Model->New->New Model：

![](/images/docs/quickstart/create_model.png)

输入Model Name点击Next进行下一步，选择Fact Table和Lookup Table，添加Lookup Table时需要设置与事实表的JOIN条件。

![](/images/docs/quickstart/add_lookup_table.png)

然后点击Next到下一步添加Dimension：

![](/images/docs/quickstart/model_add_dimension.png)

点击Next下一步添加Measure：

![](/images/docs/quickstart/model_add_measure.png)

点击Next下一步跳转到设置时间分区列和过滤条件页面，时间分区列用于增量构建时选择时间范围，如果不设置时间分区列则代表该model下的cube都是全量构建。过滤条件会在打平表时用于where条件。

![](/images/docs/quickstart/set_partition_column.png)

最后点击Save保存模型。

#### step12、创建Cube

选择Model->New->New Cube

![](/images/docs/quickstart/create_cube.png)

点击Next到下一步添加Dimension，Lookup Table的维度可以设置为Normal（普通维度）或者Derived（衍生维度）两种类型，默认设置为衍生维度，衍生维度代表该列可以从所属维度表的主键中衍生出来，所以实际上只有主键列会被Cube加入计算。

![](/images/docs/quickstart/cube_add_dimension.png)

点击Next到下一步，点击+Measure来添加需要预计算的度量。Kylin会默认创建一个Count(1)的度量。Kylin 4.x支持SUM、MIN、MAX、COUNT、COUNT_DISTINCT、TOP_N、PERCENTILE度量。请为COUNT_DISTINCT和TOP_N选择合适的返回类型，这关系到Cube的大小。添加完成之后点击ok，该Measure将会显示在Measures列表中

![](/images/docs/quickstart/cube_add_measure.png)

添加完所有Measure后点击Next进行下一步，这一页是关于Cube数据刷新的设置。在这里可以设置自动合并的阈值（Auto Merge Thresholds）、数据保留的最短时间（Retention Threshold）以及第一个Segment的起点时间。

![](/images/docs/quickstart/segment_auto_merge.png)

点击Next跳转到下一页高级设置。在这里可以设置聚合组、RowKeys、Mandatory Cuboids、Cube Engine等。

关于高级设置的详细信息，可以参考[create_cube](/cn/docs/tutorial/create_cube.html) 页面中的步骤5，其中对聚合组等设置进行了详细介绍。

关于更多维度优化，可以阅读[aggregation-group](/blog/2016/02/18/new-aggregation-group/)。 

![](/images/docs/quickstart/advance_setting.png)

对于高级设置不是很熟悉时可以先保持默认设置，点击Next跳转到Kylin Properties页面，你可以在这里重写cube级别的kylin配置项，定义覆盖的属性，配置项请参考[配置项](https://cwiki.apache.org/confluence/display/KYLIN/Configuration+Kylin+4.X)。

![](/images/docs/quickstart/properties.png)

配置完成后，点击Next按钮到下一页，这里可以预览你正在创建的Cube的基本信息，并且可以返回之前的步骤进行修改。如果没有需要修改的部分，就可以点击Save按钮完成Cube创建。之后，这个Cube将会出现在你的Cube列表中。

![](/images/docs/quickstart/cube_list.png)

#### step12、构建Cube

上一个步骤创建好的Cube只有定义，而没有计算好的数据，它的状态是‘DISABLED’，是不可以查询的。要想让Cube有数据，还需要对它进行构建。

Cube的构建方式通常有两种：全量构建和增量构建。


点击要构建的Cube的Actions列下的Action展开，选择Build，如果Cube所属Model中没有设置时间分区列，则默认全量构建，点击Submit直接提交构建任务。

如果设置了时间分区列，则会出现如下页面，在这里你要选择构建数据的起止时间：

![](/images/docs/quickstart/cube_build.png)

设置好起止时间后，点击Submit提交构建任务。然后你可以在Monitor页面观察构建任务的状态。Kylin会在页面上显示每一个步骤的运行状态、输出日志。可以在${KYLIN_HOME}/logs/kylin.log中查看更详细的日志信息。

![](/images/docs/quickstart/job_monitor4.x.png)

任务构建完成后，Cube状态会变成READY，并且可以看到Segment的信息。

![](/images/docs/quickstart/segment_info4.x.png)

#### step13、查询Cube

Cube构建完成后，在Insight页面的Tables列表下面可以看到构建完成的Cube的table，并可以对其进行查询.查询语句击中Cube后会返回存储在HDFS中的预计算结果。

![](/images/docs/quickstart/query_cube.png)

恭喜，进行到这里你已经具备了使用Kylin的基本技能，可以去发现和探索更多更强大的功能了。
