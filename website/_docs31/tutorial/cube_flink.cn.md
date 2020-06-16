---
layout: docs31-cn
title:  "用 Flink 构建 Cube"
categories: tutorial
permalink: /cn/docs31/tutorial/cube_flink.html
---
Kylin v3.1 引入了 Flink cube engine，在 build cube 步骤中使用 Apache Flink 代替 MapReduce；您可以查看 [KYLIN-3758](https://issues.apache.org/jira/browse/KYLIN-3758) 了解具体信息。当前的文档使用样例 cube 对如何尝试 new engine 进行了演示。


## 准备阶段
您需要一个安装了 Kylin v3.1.0 及以上版本的 Hadoop 环境。本文档中使用的Hadoop环境为Cloudera CDH 5.7，其中 Hadoop 组件和 Hive/HBase 已经启动了。 

## 安装 Kylin v3.1.0 及以上版本

从 Kylin 的下载页面下载适用于 CDH5.7+ 的 Kylin v3.1.0，然后在 */usr/local/* 文件夹中解压 tar 包:

{% highlight Groff markup %}

wget http://www-us.apache.org/dist/kylin/apache-kylin-3.1.0/apache-kylin-3.1.0-bin-cdh57.tar.gz -P /tmp

tar -zxvf /tmp/apache-kylin-3.1.0-bin-cdh57.tar.gz -C /usr/local/

export KYLIN_HOME=/usr/local/apache-kylin-3.1.0-bin-cdh57
{% endhighlight %}

## 准备 "kylin.env.hadoop-conf-dir"

为使 Flink 运行在 Yarn 上，需指定 **HADOOP_CONF_DIR** 环境变量，其是一个包含 Hadoop（客户端) 配置文件的目录，通常是 `/etc/hadoop/conf`。

通常 Kylin 会在启动时从 Java classpath 上检测 Hadoop 配置目录，并使用它来启动 Flink。 如果您的环境中未能正确发现此目录，那么可以显式地指定此目录：在 `kylin.properties` 中设置属性 "kylin.env.hadoop-conf-dir" 好让 Kylin 知道这个目录:

{% highlight Groff markup %}
kylin.env.hadoop-conf-dir=/etc/hadoop/conf
{% endhighlight %}

## 检查 FLink 配置

配置FLINK_HOME指向你的flink安装目录：

```$xslt
export FLINK_HOME=/path/to/flink
``` 

或者使用kylin提供的脚本下载：

```$xslt
$KYLIN_HOME/bin/download-flink.sh
```

所有使用 *"kylin.engine.flink-conf."* 作为前缀的 Flink 配置属性都能在 $KYLIN_HOME/conf/kylin.properties 中进行管理。这些属性当运行提交Flink任务时会被提取并应用。

运行 Flink cubing 前，建议查看一下这些配置并根据您集群的情况进行自定义。下面是建议配置:

{% highlight Groff markup %}
### Flink conf (default is in $FLINK_HOME/conf/flink-conf.yaml)
kylin.engine.flink-conf.jobmanager.heap.size=2G
kylin.engine.flink-conf.taskmanager.heap.size=4G
kylin.engine.flink-conf.taskmanager.numberOfTaskSlots=1
kylin.engine.flink-conf.taskmanager.memory.preallocate=false
kylin.engine.flink-conf.job.parallelism=1
kylin.engine.flink-conf.program.enableObjectReuse=false
kylin.engine.flink-conf.yarn.queue=
kylin.engine.flink-conf.yarn.nodelabel=

{% endhighlight %}

所有 "kylin.engine.flink-conf.*" 参数都可以在 Cube 或 Project 级别进行重写，这为用户提供了灵活性。

## 创建和修改样例 cube

运行 sample.sh 创建样例 cube，然后启动 Kylin 服务器:

{% highlight Groff markup %}

$KYLIN_HOME/bin/sample.sh
$KYLIN_HOME/bin/kylin.sh start

{% endhighlight %}

Kylin 启动后，访问 Kylin 网站，在 "Advanced Setting" 页，编辑名为 "kylin_sales" 的 cube，将 "Cube Engine" 由 "MapReduce" 换成 "Flink":


   ![](/images/tutorial/3.1/Flink-Cubing-Tutorial/1_flink_engine.png)

点击"Next" and "Save" 保存cube.


## 用 FLink 构建 Cube

默认情况下，只有第7步的`cube by layer`使用Flink进行构建。

点击 "Build"，选择当前日期为 end date。Kylin 会在 "Monitor" 页生成一个构建 job，第 7 步是 Flink cubing。Job engine 开始按照顺序执行每一步。 


   ![](/images/tutorial/3.1/Flink-Cubing-Tutorial/2_flink_job.png)
   
   
   ![](/images/tutorial/3.1/Flink-Cubing-Tutorial/3_flink_cubing.png)

当 Kylin 执行这一步时，您可以监视 Yarn 资源管理器里的状态. 

   ![](/images/tutorial/3.1/Flink-Cubing-Tutorial/4_job_on_yarn.png)


所有步骤成功执行后，Cube 的状态变为 "Ready" 且可以进行查询。


## 可选功能

现在构建步骤中的'extract fact table distinct value' 和 'Convert Cuboid Data to HFile' 两个步骤也可以使用Flink进行构建。相关的配置如下：

{% highlight Groff markup %}
kylin.engine.flink-fact-distinct=TRUE
kylin.engine.flink-cube-hfile=TRUE
{% endhighlight %}

## 疑难解答

当出现 error，您可以首先查看 "logs/kylin.log". 其中包含 Kylin 执行的所有 Flink 命令，例如:

{% highlight Groff markup %}
2020-06-16 15:48:05,752 INFO  [Scheduler 2113190395 Job 478f9f70-8444-6831-6817-22869f0ead2a-308] flink.FlinkExecutable:225 : cmd: export HADOOP_CONF_DIR=/etc/hadoop/conf && export HADOOP_CLASSPATH=/etc/hadoop && /root/apache-kylin-3.1.0-SNAPSHOT-bin-master/flink/bin/flink run -m yarn-cluster  -ytm 4G -yjm 2G -yD taskmanager.memory.preallocate false -ys 1 -c org.apache.kylin.common.util.FlinkEntry -p 1 /root/apache-kylin-3.1.0-SNAPSHOT-bin/lib/kylin-job-3.1.0-SNAPSHOT.jar -className org.apache.kylin.engine.flink.FlinkCubingByLayer -hiveTable default.kylin_intermediate_kylin_sales_cube_flink_75ffb8ff_b27c_c86b_70f2_832a4f18f5cf -output hdfs://cdh-master:8020/kylin/yaqian/kylin_zyq/kylin-478f9f70-8444-6831-6817-22869f0ead2a/kylin_sales_cube_flink/cuboid/ -input hdfs://cdh-master:8020/kylin/yaqian/kylin_zyq/kylin-478f9f70-8444-6831-6817-22869f0ead2a/kylin_intermediate_kylin_sales_cube_flink_75ffb8ff_b27c_c86b_70f2_832a4f18f5cf -enableObjectReuse false -segmentId 75ffb8ff-b27c-c86b-70f2-832a4f18f5cf -metaUrl kylin_zyq@hdfs,path=hdfs://cdh-master:8020/kylin/yaqian/kylin_zyq/kylin-478f9f70-8444-6831-6817-22869f0ead2a/kylin_sales_cube_flink/metadata -cubename kylin_sales_cube_flink

{% endhighlight %}

您可以复制 cmd 以便在 shell 中手动执行，然后快速进行参数调整；执行期间，您可以访问 Yarn 资源管理器查看更多信息。如果 job 已经完成了，您可以检查flink的日志文件。 

## 更多

如果您对 Kylin 很熟悉但是对于 Flink 是新手，建议您浏览 [Flink 文档](https://flink.apache.org)，根据文档相应地去更新配置。
如果您有任何问题，意见，或 bug 修复，欢迎在 dev@kylin.apache.org 中讨论。
