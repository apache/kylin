#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# syntax=docker/dockerfile:1
FROM ubuntu:22.04
RUN apt update
RUN apt install -y openjdk-8-jdk vim less net-tools lsof ssh curl
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# ssh config
COPY conf/ssh/sshd.conf /etc/ssh/sshd_config.d/
COPY conf/ssh/ssh.conf /etc/ssh/ssh_config.d/
RUN ssh-keygen -t ed25519 -P '' -f ~/.ssh/id_ed25519
RUN cat ~/.ssh/id_ed25519.pub > ~/.ssh/authorized_keys
RUN chmod 0400 ~/.ssh/authorized_keys

# install mysql
ADD scripts/install-mysql.sh /opt/
RUN chmod 0755 /opt/install-mysql.sh
RUN /bin/bash /opt/install-mysql.sh

# install hadoop
ENV HADOOP_VERSION=3.2.4
ENV HIVE_VERSION=3.1.3
ENV ZOOKEEPER_VERSION=3.7.2

ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HIVE_HOME=/opt/apache-hive-$HIVE_VERSION-bin
ENV ZOOKEEPER_HOME=/opt/apache-zookeeper-$ZOOKEEPER_VERSION-bin

WORKDIR /opt
ADD https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz /opt/
ADD https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz /opt/
ADD https://archive.apache.org/dist/zookeeper/zookeeper-$ZOOKEEPER_VERSION/apache-zookeeper-$ZOOKEEPER_VERSION-bin.tar.gz /opt/
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar /opt/
RUN tar -zxf /opt/hadoop-$HADOOP_VERSION.tar.gz
RUN tar -zxf /opt/apache-hive-$HIVE_VERSION-bin.tar.gz
RUN tar -zxf /opt/apache-zookeeper-$ZOOKEEPER_VERSION-bin.tar.gz
RUN rm -f /opt/hadoop-$HADOOP_VERSION.tar.gz
RUN rm -f /opt/apache-hive-$HIVE_VERSION-bin.tar.gz
RUN rm -f /opt/apache-zookeeper-$ZOOKEEPER_VERSION-bin.tar.gz

RUN mkdir -p /data/hadoop
RUN mkdir -p /data/zookeeper
COPY scripts/start-historyserver.sh $HADOOP_HOME/sbin/
RUN chmod 0755 $HADOOP_HOME/sbin/start-historyserver.sh
COPY conf/hadoop/* $HADOOP_HOME/etc/hadoop/
COPY scripts/start-hivemetastore.sh $HIVE_HOME/bin/
COPY scripts/start-hiveserver2.sh $HIVE_HOME/bin/
RUN chmod 0755 $HIVE_HOME/bin/start-hivemetastore.sh
RUN chmod 0755 $HIVE_HOME/bin/start-hiveserver2.sh
COPY conf/hive/* $HIVE_HOME/conf/
COPY conf/zookeeper/* $ZOOKEEPER_HOME/conf/
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin

# hive lib extra
RUN rm -f $HIVE_HOME/lib/guava-*.jar
RUN cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/
RUN cp /opt/mysql-connector-j-8.0.33.jar $HIVE_HOME/lib/

# install apache kylin
RUN mkdir -p /home/kylin
ADD package/apache-kylin-5.0.0-GA-bin.tar.gz /home/kylin
ENV KYLIN_HOME=/home/kylin/apache-kylin-5.0.0-GA-bin
RUN cp /opt/mysql-connector-j-8.0.33.jar $KYLIN_HOME/lib/ext/
COPY conf/kylin/kylin.properties.override $KYLIN_HOME/conf/

# startup script
COPY scripts/entrypoint.sh /home/kylin/
RUN chmod 0755 /home/kylin/entrypoint.sh

WORKDIR /home/kylin
CMD ["/bin/bash", "entrypoint.sh"]
