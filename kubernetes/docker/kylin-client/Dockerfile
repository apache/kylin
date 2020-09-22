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

FROM hadoop-client:cdh57

MAINTAINER Apache Kylin

ARG USER=apache_kylin
ARG USER_HOME=/home/${USER}
ARG KYLIN_VERSION=apache-kylin-3.0.1-bin-cdh57

ARG HADOOP_CONF_HOME=/etc/hadoop/conf
ARG HIVE_CONF_HOME=/etc/hive/conf
ARG HBASE_CONF_HOME=/etc/hbase/conf
ARG SPARK_CONF_HOME=$KYLIN_HOME/hadoop-conf

ENV SPARK_HOME /opt/spark-2.3.2-bin-hadoop2.7
ENV KYLIN_HOME=$USER_HOME/kylin

WORKDIR /tmp

# install system tools
RUN set -x \
    && yum install -y openssh-clients \
       cronie \
       unzip \
       sudo \
       net-tools \
       iftop \
       tcpdump \
       perf \
       telnet \
       bind-utils \
    && yum clean all

RUN set -x \
    && groupadd -r $USER \
    && useradd -r -m -g $USER $USER -d $USER_HOME \
    && echo '$USER ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

COPY --chown=$USER:$USER $KYLIN_VERSION $KYLIN_HOME

RUN set -x \
    && unzip -qq $KYLIN_HOME/tomcat/webapps/kylin.war -d $KYLIN_HOME/tomcat/webapps/kylin \
    && chown -R $USER:$USER $KYLIN_HOME/tomcat/webapps/kylin \
    && rm $KYLIN_HOME/tomcat/webapps/kylin.war \
    && mkdir $SPARK_CONF_HOME \
    && ln -s $HADOOP_CONF_HOME/core-site.xml $SPARK_CONF_HOME/core-site.xml \
    && ln -s $HADOOP_CONF_HOME/hdfs-site.xml $SPARK_CONF_HOME/hdfs-site.xml \
    && ln -s $HADOOP_CONF_HOME/yarn-site.xml $SPARK_CONF_HOME/yarn-site.xml \
    && ln -s $HADOOP_CONF_HOME/hdfs-variable.xml $SPARK_CONF_HOME/hdfs-variable.xml\
    && ln -s $HADOOP_CONF_HOME/yarn-variable.xml $SPARK_CONF_HOME/yarn-variable.xml\
    && ln -s $HADOOP_CONF_HOME/federation-mapping.xml $SPARK_CONF_HOME/federation-mapping.xml\
    && ln -s $HIVE_CONF_HOME/hive-site.xml $SPARK_CONF_HOME/hive-site.xml \
    && ln -s $HBASE_CONF_HOME/hbase-site.xml $SPARK_CONF_HOME/hbase-site.xml \
    && chown -R $USER:$USER $SPARK_CONF_HOME

ENV TOOL_HOME=$USER_HOME/bin
COPY bin $TOOL_HOME
COPY crontab.txt /tmp/crontab.txt

RUN /usr/bin/crontab -u $USER /tmp/crontab.txt \
    && rm -rf /tmp/* \
    && chmod 755 $TOOL_HOME/*
EXPOSE 7070

USER $USER
CMD ["sh", "-c", "$TOOL_HOME/bootstrap.sh server"]