---
layout: dev40-cn
title:  搭建开发环境
categories: development
permalink: /cn/development40/dev_env.html
---

开发者想要在他们的开发机器上运行 Kylin4 的测试用例或应用。

跟随这个教程，您可以很方便的在本地机器上搭建一个 Kylin4 的开发环境，不需要连接 Hadoop 客户端或者沙箱。

## 开发机器的环境

### 安装 Maven

下载 Maven 3.5.4 及以上版本：<http://maven.apache.org/download.cgi>，然后创建一个软链接，以便 `mvn` 可以在任何地方运行。

{% highlight Groff markup %}
cd ~
wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz
tar -xzvf apache-maven-3.5.4-bin.tar.gz
ln -s /root/apache-maven-3.5.4/bin/mvn /usr/bin/mvn
{% endhighlight %}

### 安装 Spark

在像 /usr/local/spark 这样的本地文件夹下手动安装 Spark；Kylin4 支持 Spark 2.4.7，你需要从 Spark 下载页面获取下载链接。 

{% highlight Groff markup %}
wget -O /tmp/spark-2.4.7-bin-hadoop2.7.tgz https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
cd /usr/local
tar -zxvf /tmp/spark-2.4.7-bin-hadoop2.7.tgz
ln -s spark-2.4.7-bin-hadoop2.7 spark
{% endhighlight %}

### 编译

首先将 Kylin 工程下载到本地：

{% highlight Groff markup %}
git clone https://github.com/apache/kylin.git
{% endhighlight %}
	
将 Kylin 工件安装到 Maven 仓库：

{% highlight Groff markup %}
mvn clean install -DskipTests
{% endhighlight %}


### 运行单元测试
运行单元测试来测试每一个 classes 基本功能的有效性。

{% highlight Groff markup %}
mvn clean test
{% endhighlight %}

### 运行集成测试
执行以下命令，将会运行单元测试和集成测试。提交代码前，单元测试和集成测试都需要运行通过。

{% highlight Groff markup %}
mvn clean test -DskipRunIt=false
{% endhighlight %}

关于测试想要了解更多，请参考[如何测试](/cn/development40/howto_test.html).

### 本地运行 Kylin Web 服务器

拷贝 server/src/main/webapp/WEB-INF 到 webapp/app/WEB-INF 

{% highlight Groff markup %}
cp -r server/src/main/webapp/WEB-INF webapp/app/WEB-INF 
{% endhighlight %}

为 Kylin web GUI 下载 JS。`npm` 是 `Node.js` 的一部分，请搜索有关如何在您的操作系统上安装它的信息。

{% highlight Groff markup %}
cd webapp
npm install -g bower
bower --allow-root install
{% endhighlight %}

如果在 bower install 的过程当中遇到问题，可以尝试命令：

{% highlight Groff markup %}
git config --global url."git://".insteadOf https://
{% endhighlight %}

如因网络问题或者包在默认仓库内无法获取导致安装失败，可参考[如何设置Kylin的前端仓库](/cn/development40/howto_setup_frontend_registry.html)配置Kylin前端仓库

注意，如果是在 Windows 上，安装完 bower，需要将 "bower.cmd" 的路径加入系统环境变量 'PATH' 中，然后运行：

{% highlight Groff markup %}
bower.cmd --allow-root install
{% endhighlight %}

在配置文件 **examples/test_case_data/sandbox/kylin.properties** 中找到以下配置，并按照下面的的示例修改：

```
# Need to use absolute pat
kylin.metadata.url=${KYLIN_SOURCE_DIR}/examples/test_case_data/sample_local
kylin.storage.url=${KYLIN_SOURCE_DIR}/examples/test_case_data/sample_local
kylin.env.zookeeper-is-local=true
kylin.env.hdfs-working-dir=file://$KYLIN_SOURCE_DIR/examples/test_case_data/sample_local
kylin.engine.spark-conf.spark.master=local
# Need to create `/path/to/local/dir` manually
kylin.engine.spark-conf.spark.eventLog.dir=/path/to/local/dir
kylin.engine.spark-conf.spark.sql.shuffle.partitions=1
kylin.env=LOCAL
```

在 IDE，运行 `org.apache.kylin.rest.DebugTomcat`。将工作目录设置为 /server 文件夹，使用 "kylin-server" 的 classpath。在运行之前，请在 IDE 安装 Scala 插件，以保证能够编译 Spark 代码。对于 IntelliJ IDEA 2017 或之前的用户，需要修改 "server/kylin-server.iml" 文件，将所有的 "PROVIDED" 替换为 "COMPILE"；对于 IntelliJ IDEA 2018 用户，请勾选 “Include dependencies with 'Provided' scope”，否则可能会抛出 "java.lang.NoClassDefFoundError: org/apache/catalina/LifecycleListener" 错误。

并调节 VM options:

```
-Dspark.local=true
```

![DebugTomcat Config](/images/develop40/debug_tomcat_config.png)

`DebugTomcat` 运行成功后，查看 Kylin Web `http://localhost:7070/kylin`（用户名：ADMIN，密码：KYLIN)

## 安装 IDE 编码格式化工具

如果你正在为 Kylin 编写代码，你应该确保你的代码符合预期的格式。

对于 Eclipse 用户，只需在提交代码之前格式化代码。

对于 intellij IDEA 用户，您还需要执行一些额外步骤：

1. 安装 "Eclipse Code Formatter" 并在 core-common/.settings 中使用 "org.eclipse.jdt.core.prefs" 和 "org.eclipse.jdt.ui.prefs" 来配置 "Eclipse Java Formatter config file" 和 "Import order"

	![Eclipse_Code_Formatter_Config](/images/develop/eclipse_code_formatter_config.png)

2. 去 Preference => Code Style => Java，将 "Scheme" 设为默认，并设置 "Class count to use import with '\*'" 和 "Names count to use static import with '\*'" 为 99。

	![Kylin_Intellj_Code_Style](/images/develop/kylin-intellij-code-style.png)

3. 禁用 intellij IDEA 的 "Optimize imports on the fly"

	![Disable_Optimize_On_The_Fly](/images/develop/disable_import_on_the_fly.png)

3. 提交代码前格式化代码。

## 设置 IDE license 头部模板

每一个源文件都应该包括以下的 Apache License 头部
{% highlight Groff markup %}
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endhighlight %}

当打包时 checkstyle 插件将会检查头部规则。license 文件在 `dev-support/checkstyle-apache-header.txt`。为了方便开发人员，请将头部添加为 `Copyright Profile`，并将其设置为 Kylin 项目的默认值。
![Apache License Profile](/images/develop/intellij_apache_license.png)
