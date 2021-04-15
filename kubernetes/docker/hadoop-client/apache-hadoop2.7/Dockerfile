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

FROM centos:7.3.1611

MAINTAINER Apache Kylin

WORKDIR /tmp

# install jdk and other commands
RUN set -x \
    && yum install -y which \
       java-1.8.0-openjdk \
       java-1.8.0-openjdk-devel \
       krb5-workstation \
    && yum clean all


# version variables
ARG HADOOP_VERSION=2.7.4
ARG HIVE_VERSION=1.2.2
ARG HBASE_VERSION=1.3.1
ARG SPARK_VERSION=2.2.1
ARG ZK_VERSION=3.4.6

ARG APACHE_HOME=/apache

RUN set -x \
    && mkdir -p $APACHE_HOME

ENV JAVA_HOME /etc/alternatives/jre

# install hadoop
ENV HADOOP_HOME=$APACHE_HOME/hadoop
ADD hadoop-binary/hadoop-${HADOOP_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/hadoop-${HADOOP_VERSION} $HADOOP_HOME

#install hive
ENV HIVE_HOME=$APACHE_HOME/hive 
ADD hadoop-binary/apache-hive-${HIVE_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/apache-hive-${HIVE_VERSION} $HIVE_HOME

#install hbase
ENV HBASE_HOME=$APACHE_HOME/hbase
ADD hadoop-binary/hbase-${HBASE_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/hbase-${HBASE_VERSION} $HBASE_HOME

#install spark
ENV SPARK_HOME=$APACHE_HOME/spark
ADD hadoop-binary/spark-${SPARK_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/spark-${SPARK_VERSION} $SPARK_HOME

#install zk
ENV ZK_HOME=$APACHE_HOME/zookeeper
ADD hadoop-binary/zookeeper-${ZK_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/zookeeper-${ZK_VERSION} $ZK_HOME

ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$HBASE_HOME/bin:$ZK_HOME/bin

# Cleanup
RUN rm -rf /tmp/*
