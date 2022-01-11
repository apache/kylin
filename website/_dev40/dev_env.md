---
layout: dev40
title:  Setup Development Env
categories: development
permalink: /development40/dev_env.html
---

Developers want to run Kylin4 test cases or applications at their development machine. 

Following this tutorial, you can easily build a kylin4 development environment on your local machine without connecting to a Hadoop client or sandbox.

## Environment on the dev machine


### Install Maven

Download Maven 3.5.4 and above version: <http://maven.apache.org/download.cgi>, we create a symbolic so that `mvn` can be run anywhere.

{% highlight Groff markup %}
cd ~
wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz
tar -xzvf apache-maven-3.5.4-bin.tar.gz
ln -s /root/apache-maven-3.5.4/bin/mvn /usr/bin/mvn
{% endhighlight %}

### Install Spark

Manually install the Spark binary in a local folder like /usr/local/spark. Kylin4 supports Spark 2.4.7, you need to get the download link from the spark download page.

{% highlight Groff markup %}
wget -O /tmp/spark-2.4.7-bin-hadoop2.7.tgz https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
cd /usr/local
tar -zxvf /tmp/spark-2.4.7-bin-hadoop2.7.tgz
ln -s spark-2.4.7-bin-hadoop2.7 spark
{% endhighlight %}

### Compile

First clone the Kylin project to your local:

{% highlight Groff markup %}
git clone https://github.com/apache/kylin.git
{% endhighlight %}
	
Install Kylin artifacts to the maven repository.

{% highlight Groff markup %}
mvn clean install -DskipTests
{% endhighlight %}

### Run unit tests
Run unit tests to validate basic function of each classes.

{% highlight Groff markup %}
mvn clean test
{% endhighlight %}

### Run integration tests
Executing the following command will run the unit test and integration test. Both unit and integration tests need to be passed before code is submitted.

{% highlight Groff markup %}
mvn clean test -DskipRunIt=false
{% endhighlight %}

To learn more about test, please refer to [How to test](/development40/howto_test.html).

### Launch Kylin Web Server locally

Copy server/src/main/webapp/WEB-INF to webapp/app/WEB-INF 

{% highlight Groff markup %}
cp -r server/src/main/webapp/WEB-INF webapp/app/WEB-INF 
{% endhighlight %}

Download JS for Kylin web GUI. `npm` is part of `Node.js`, please search about how to install it on your OS.

{% highlight Groff markup %}
cd webapp
npm install -g bower
bower --allow-root install
{% endhighlight %}

If you encounter network problem when run "bower install", you may try:

{% highlight Groff markup %}
git config --global url."git://".insteadOf https://
{% endhighlight %}

If errors occur during installing Kylin's frontend dependencies due to network latency or some packages not obtainable by default registry, please refer to [How to Set up Frontend Registry](/development40/howto_setup_frontend_registry.html)

Note, if on Windows, after install bower, need to add the path of "bower.cmd" to system environment variable 'PATH', and then run:

{% highlight Groff markup %}
bower.cmd --allow-root install
{% endhighlight %}

Find the following configuration in **examples/test_case_data/sandbox/kylin.properties** and modify them according to the following example:

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

In IDE, launch `org.apache.kylin.rest.DebugTomcat`. Please set the path of "server" module as the "Working directory", set "kylin-server" for "Use classpath of module", and check "Include dependencies with 'Provided' scope" option in IntelliJ IDEA 2018. If you're using IntelliJ IDEA 2017 and older, you need modify "server/kylin-server.iml" file, replace all "PROVIDED" to "COMPILE", otherwise an "java.lang.NoClassDefFoundError: org/apache/catalina/LifecycleListener" error may be thrown.. 

And adjust VM options:

```
-Dspark.local=true
```

![DebugTomcat Config](/images/develop40/debug_tomcat_config.png)


By default Kylin server will listen on 7070 port; If you want to use another port, please specify it as a parameter when run `DebugTomcat`.

Check Kylin Web at `http://localhost:7070/kylin` (user:ADMIN, password:KYLIN)


## Setup IDE code formatter

In case you're writting code for Kylin, you should make sure that your code in expected formats.

For Eclipse users, just format the code before committing the code.

For intellij IDEA users, you have to do a few more steps:

1. Install "Eclipse Code Formatter" and use "org.eclipse.jdt.core.prefs" and "org.eclipse.jdt.ui.prefs" in core-common/.settings to configure "Eclipse Java Formatter config file" and "Import order"

	![Eclipse_Code_Formatter_Config](/images/develop/eclipse_code_formatter_config.png)

2. Go to Preference => Code Style => Java, set "Scheme" to Default, and set both "Class count to use import with '\*'" and "Names count to use static import with '\*'" to 99.

	![Kylin_Intellj_Code_Style](/images/develop/kylin-intellij-code-style.png)

3. Disable intellij IDEA's "Optimize imports on the fly"

	![Disable_Optimize_On_The_Fly](/images/develop/disable_import_on_the_fly.png)

3. Format the code before committing the code.

## Setup IDE license header template

Each source file should include the following Apache License header
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

The checkstyle plugin will check the header rule when packaging also. The license file locates under `dev-support/checkstyle-apache-header.txt`. To make it easy for developers, please add the header as `Copyright Profile` and set it as default for Kylin project.
![Apache License Profile](/images/develop/intellij_apache_license.png)