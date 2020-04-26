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

ENV JAVA_VERSION    1.8.0
ENV SPARK_VERSION   2.3.2

ENV JAVA_HOME       /usr/lib/jvm/java-${JAVA_VERSION}
ENV HADOOP_HOME     /usr/lib/hadoop
ENV HIVE_HOME       /usr/lib/hive
ENV HCAT_HOME       /usr/lib/hive-hcatalog
ENV HBASE_HOME      /usr/lib/hbase
ENV SPARK_HOME      /opt/spark-${SPARK_VERSION}-bin-hadoop2.7

ENV PATH $PATH:$SPARK_HOME/bin:

ENV HADOOP_CONF_DIR  /etc/hadoop/conf
ENV HIVE_CONF_DIR    /etc/hive/conf
ENV HBASE_CONF_DIR   /etc/hbase/conf
ENV HIVE_CONF        ${HIVE_CONF_DIR}
ENV HIVE_LIB         ${HIVE_HOME}/lib

RUN echo $'[cloudera-cdh5] \n\
# Packages for Cloudera\'s Distribution for Hadoop, Version 5, on RedHat or CentOS 6 x86_64 \n\
name=Cloudera\'s Distribution for Hadoop, Version 5 \n\
baseurl=https://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/5.7.6/ \n\
gpgkey =https://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera \n\
gpgcheck = 1' > /etc/yum.repos.d/cloudera-cdh5.repo

WORKDIR /opt

RUN set -xeu && yum -y  install java-1.8.0-openjdk-devel
RUN set -xeu && yum -y  install krb5-workstation
RUN set -xeu && yum -y  install hadoop-client
RUN set -xeu && yum -y  install hive hive-hcatalog
RUN set -xeu && yum -y  install hbase

RUN set -xeu && \
    curl -o ${HIVE_HOME}/lib/hadoop-lzo-0.4.15.jar \
    https://clojars.org/repo/hadoop-lzo/hadoop-lzo/0.4.15/hadoop-lzo-0.4.15.jar && \
    curl -o ${HIVE_HOME}/lib/mysql-connector-java-5.1.24.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.24/mysql-connector-java-5.1.24.jar

RUN set -xeu && \
    yum -q clean all && \
    rm -rf /var/cache/yum && \
    rm -rf /tmp/* /var/tmp/*

ADD provided-binary/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz .