---
layout: docs31-cn
title:  "从 Kafka 流构建 Cube"
categories: tutorial
permalink: /cn/docs31/tutorial/cube_streaming.html
---
Kylin v1.6 发布了可扩展的 streaming cubing 功能，它利用 Hadoop 消费 Kafka 数据的方式构建 cube，您可以查看 [这篇博客](/blog/2016/10/18/new-nrt-streaming/) 以进行高级别的设计。本文档是一步接一步的阐述如何创建和构建样例 cube 的教程;

## 前期准备
您需要一个安装了 kylin v1.6.0 或以上版本和可运行的 Kafka; 自 kylin v2.5 开始，需要 Kafka v1.0.0 或以上版本。

本教程中我们使用 Hortonworks HDP 2.2.4 Sandbox VM + Kafka v1.0.2(Scala 2.11) 作为环境。

## 安装 Kafka 1.0.2 和 Kylin
不要使用 HDP 2.2.4 自带的 Kafka，因为它太旧了，如果其运行着请先停掉。然后前往 Kafka 项目下载其二进制包到本地 /usr/local/。
{% highlight Groff markup %}
tar -zxvf kafka_2.11-1.0.2.tgz
cd kafka_2.11-1.0.2

bin/kafka-server-start.sh config/server.properties &

{% endhighlight %}

从下载页下载 Kylin，在 /usr/local/ 文件夹中解压 tar 包。

## 创建样例 Kafka topic 并填充数据

创建样例名为 "kylin_streaming_topic" 具有三个分区的 topic:

{% highlight Groff markup %}

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic kylin_streaming_topic
Created topic "kylin_streaming_topic".
{% endhighlight %}

将样例数据放入 topic；Kylin 有一个实用类可以做这项工作;

{% highlight Groff markup %}
export KAFKA_HOME=/usr/local/kafka_2.11-1.0.2
export KYLIN_HOME=/usr/local/apache-kylin-2.6.0-bin

cd $KYLIN_HOME
./bin/kylin.sh org.apache.kylin.source.kafka.util.KafkaSampleProducer --topic kylin_streaming_topic --broker localhost:9092
{% endhighlight %}

工具每一秒会向 Kafka 发送 100 条记录 （v2.6.0 此处有一个bug：KYLIN-3793）。直至本教程结束请让其一直运行。现在您可以用 kafka-console-consumer.sh 查看样例消息:

{% highlight Groff markup %}
cd $KAFKA_HOME
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kylin_streaming_topic --from-beginning
{"amount":63.50375137330458,"category":"TOY","order_time":1477415932581,"device":"Other","qty":4,"user":{"id":"bf249f36-f593-4307-b156-240b3094a1c3","age":21,"gender":"Male"},"currency":"USD","country":"CHINA"}
{"amount":22.806058795736583,"category":"ELECTRONIC","order_time":1477415932591,"device":"Andriod","qty":1,"user":{"id":"00283efe-027e-4ec1-bbed-c2bbda873f1d","age":27,"gender":"Female"},"currency":"USD","country":"INDIA"}

 {% endhighlight %}

## 用 streaming 定义一张表
用 "$KYLIN_HOME/bin/kylin.sh start" 启动 Kylin 服务器，输入 http://sandbox:7070/kylin/ 登陆 Kylin Web GUI，选择一个已存在的 project 或创建一个新的 project；点击 "Model" -> "Data Source"，点击 "Add Streaming Table" 图标;

   ![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/1_Add_streaming_table.png)

在弹出的对话框中，输入您从 kafka-console-consumer 中获得的样例记录，点击 ">>" 按钮，Kylin 会解析 JSON 消息并列出所有的消息;

您需要为这个 streaming 数据源起一个逻辑表名；该名字会在后续用于 SQL 查询；这里是在 "Table Name" 字段输入 "STREAMING_SALES_TABLE" 作为样例。

您需要选择一个时间戳字段用来标识消息的时间；Kylin 可以从这列值中获得其他时间值，如 "year_start"，"quarter_start"，这为您构建和查询 cube 提供了更高的灵活性。这里可以查看 "order_time"。您可以取消选择那些 cube 不需要的属性。这里我们保留了所有字段。

注意 Kylin 从 1.6 版本开始支持结构化 (或称为 "嵌入") 消息，会将其转换成一个 flat table structure。默认使用 "_" 作为结构化属性的分隔符。

   ![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/2_Define_streaming_table.png)


点击 "Next"。在这个页面，提供了 Kafka 集群信息；输入 "kylin_streaming_topic" 作为 "Topic" 名；集群有 1 个 broker，其主机名为 "sandbox"，端口为 "9092"，点击 "Save"。

   ![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/3_Kafka_setting.png)

在 "Advanced setting" 部分，"timeout" 和 "buffer size" 是和 Kafka 进行连接的配置，保留它们。 

在 "Parser Setting"，Kylin 默认您的消息为 JSON 格式，每一个记录的时间戳列 (由 "tsColName" 指定) 是 bigint (新纪元时间) 类型值；在这个例子中，您只需设置 "tsColumn" 为 "order_time"；

![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/3_Paser_setting.png)

在现实情况中如果时间戳值为 string 如 "Jul 20，2016 9:59:17 AM"，您需要用 "tsParser" 指定解析类和时间模式例如:


![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/3_Paser_time.png)

点击 "Submit" 保存设置。现在 "Streaming" 表就创建好了。

![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/4_Streaming_table.png)

## 定义数据模型
有了上一步创建的表，现在我们可以创建数据模型了。步骤和您创建普通数据模型是一样的，但有两个要求:

* 在 v2.4.0 以前，Streaming Cube 不支持与 lookup 表进行 join；当定义数据模型时，只选择 fact 表，不选 lookup 表;
* 如果您使用的是 v2.4.0 或更高版本，可以添加多个 lookup 表到模型中， 所有 lookup 表需要是 Hive 表;
* Streaming Cube 必须进行分区；如果您想要在分钟级别增量的构建 Cube，选择 "MINUTE_START" 作为 cube 的分区日期列。如果是在小时级别，选择 "HOUR_START"。

这里我们选择 13 个 dimension 和 2 个 measure 列:

![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/5_Data_model_dimension.png)

![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/6_Data_model_measure.png)
保存数据模型。

## 创建 Cube

Streaming Cube 和普通的 cube 大致上一样. 有以下几点需要您注意:

* 分区时间列应该是 Cube 的一个 dimension。在 Streaming OLAP 中时间总是一个查询条件，Kylin 利用它来缩小扫描分区的范围。
* 不要使用 "order\_time" 作为 dimension 因为它非常的精细；建议使用 "mintue\_start"，"hour\_start" 或其他，取决于您如何检查数据。
* 定义 "year\_start"，"quarter\_start"，"month\_start"，"day\_start"，"hour\_start"，"minute\_start" 作为层级以减少组合计算。
* 在 "refersh setting" 这一步，创建更多合并的范围，如 0.5 小时，4 小时，1 天，然后是 7 天；这将会帮助您控制 cube segment 的数量。
* 在 "rowkeys" 部分，拖拽 "minute\_start" 到最上面的位置，对于 streaming 查询，时间条件会一直显示；将其放到前面将会帮助您缩小扫描范围。

	![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/8_Cube_dimension.png)

	![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/9_Cube_measure.png)

	![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/10_agg_group.png)

	![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/11_Rowkey.png)

保存 cube。

## 运行 build

您可以在 web GUI 触发 build，通过点击 "Actions" -> "Build"，或用 'curl' 命令发送一个请求到 Kylin RESTful API:

{% highlight Groff markup %}
curl -X PUT --user ADMIN:KYLIN -H "Content-Type: application/json;charset=utf-8" -d '{ "sourceOffsetStart": 0, "sourceOffsetEnd": 9223372036854775807, "buildType": "BUILD"}' http://localhost:7070/kylin/api/cubes/{your_cube_name}/build2
{% endhighlight %}

请注意 API 终端和普通 cube 不一样 (这个 URL 以 "build2" 结尾)。

这里的 0 表示从最后一个位置开始，9223372036854775807 (Long 类型的最大值) 表示到 Kafka topic 的结束位置。如果这是第一次 build (没有以前的 segment)，Kylin 将会寻找 topics 的开头作为开始位置。 

在 "Monitor" 页面，一个新的 job 生成了；等待其直到 100% 完成。

## 点击 "Insight" 标签，编写 SQL 运行，例如:

 {% highlight Groff markup %}
select minute_start, count(*), sum(amount), sum(qty) from streaming_sales_table group by minute_start order by minute_start
 {% endhighlight %}

结果如下。
![](/images/tutorial/1.6/Kylin-Cube-Streaming-Tutorial/13_Query_result.png)


## 自动 build

一旦第一个 build 和查询成功了，您可以按照一定的频率调度增量 build。Kylin 将会记录每一个 build 的 offsets；当收到一个 build 请求，它将会从上一个结束的位置开始，然后从 Kafka 获取最新的 offsets。有了 REST API 您可以使用任何像 Linux cron 调度工具触发它:

  {% highlight Groff markup %}
crontab -e
*/5 * * * * curl -X PUT --user ADMIN:KYLIN -H "Content-Type: application/json;charset=utf-8" -d '{ "sourceOffsetStart": 0, "sourceOffsetEnd": 9223372036854775807, "buildType": "BUILD"}' http://localhost:7070/kylin/api/cubes/{your_cube_name}/build2
 {% endhighlight %}

现在您可以观看 cube 从 streaming 中自动 built。当 cube segments 累积到更大的时间范围，Kylin 将会自动的将其合并到一个更大的 segment 中。

## 疑难解答

 * 运行 "kylin.sh" 时您可能遇到以下错误:
{% highlight Groff markup %}
Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/kafka/clients/producer/Producer
	at java.lang.Class.getDeclaredMethods0(Native Method)
	at java.lang.Class.privateGetDeclaredMethods(Class.java:2615)
	at java.lang.Class.getMethod0(Class.java:2856)
	at java.lang.Class.getMethod(Class.java:1668)
	at sun.launcher.LauncherHelper.getMainMethod(LauncherHelper.java:494)
	at sun.launcher.LauncherHelper.checkAndLoadMain(LauncherHelper.java:486)
Caused by: java.lang.ClassNotFoundException: org.apache.kafka.clients.producer.Producer
	at java.net.URLClassLoader$1.run(URLClassLoader.java:366)
	at java.net.URLClassLoader$1.run(URLClassLoader.java:355)
	at java.security.AccessController.doPrivileged(Native Method)
	at java.net.URLClassLoader.findClass(URLClassLoader.java:354)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:425)
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:308)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:358)
	... 6 more
{% endhighlight %}

原因是 Kylin 不能找到正确的 Kafka client jars；确保您设置了正确的 "KAFKA_HOME" 环境变量。

 * "Build Cube" 步骤中的 "killed by admin" 错误 

 在 Sandbox VM 中，YARN 不能给 MR job 分配请求的内存资源，因为 "inmem" cubing 算法需要更多的内存。您可以通过请求更少的内存来绕过这一步: 编辑 "conf/kylin_job_conf_inmem.xml"，将这两个参数改为如下这样:

 {% highlight Groff markup %}
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>1072</value>
        <description></description>
    </property>

    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx800m</value>
        <description></description>
    </property>
 {% endhighlight %}

 * 如果 Kafka 里已经有一组历史 message 且您不想从最开始 build，您可以触发一个调用来将当前的结束位置设为 cube 的开始:

{% highlight Groff markup %}
curl -X PUT --user ADMIN:KYLIN -H "Content-Type: application/json;charset=utf-8" -d '{ "sourceOffsetStart": 0, "sourceOffsetEnd": 9223372036854775807, "buildType": "BUILD"}' http://localhost:7070/kylin/api/cubes/{your_cube_name}/init_start_offsets
{% endhighlight %}

 * 如果一些 build job 出错了并且您将其 discard，Cube 中就会留有一个洞（或称为空隙）。每一次 Kylin 都会从最后的位置 build，您不可期望通过正常的 builds 将洞填补。Kylin 提供了 API 检查和填补洞 

检查洞:
 {% highlight Groff markup %}
curl -X GET --user ADMIN:KYLIN -H "Content-Type: application/json;charset=utf-8" http://localhost:7070/kylin/api/cubes/{your_cube_name}/holes
{% endhighlight %}

如果查询结果是一个空的数组，意味着没有洞；否则，触发 Kylin 填补他们:
 {% highlight Groff markup %}
curl -X PUT --user ADMIN:KYLIN -H "Content-Type: application/json;charset=utf-8" http://localhost:7070/kylin/api/cubes/{your_cube_name}/holes
{% endhighlight %}

