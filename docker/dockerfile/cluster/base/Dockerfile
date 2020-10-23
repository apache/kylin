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
MAINTAINER kylin

USER root

ARG JAVA_VERSION=jdk1.8.0_141
ARG HADOOP_VERSION=2.8.5
ARG INSTALL_FROM=local
ARG HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

ENV JAVA_HOME /opt/${JAVA_VERSION}
ENV HADOOP_VERSION ${HADOOP_VERSION}
ENV INSTALL_FROM ${INSTALL_FROM}
ENV HADOOP_URL ${HADOOP_URL}

# install tools
RUN yum -y install lsof wget tar git unzip wget curl net-tools procps perl sed nc which

# setup jdk
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.tar.gz" -P /opt \
    && tar -zxvf /opt/jdk-8u141-linux-x64.tar.gz -C /opt/ \
    && rm -f /opt/jdk-8u141-linux-x64.tar.gz

RUN set -x \
    && echo "Fetch URL2 is : ${HADOOP_URL}" \
    && curl -fSL "${HADOOP_URL}" -o /tmp/hadoop.tar.gz \
    && curl -fSL "${HADOOP_URL}.asc" -o /tmp/hadoop.tar.gz.asc

RUN set -x \
    && tar -xvf /tmp/hadoop.tar.gz -C /opt/ \
    && rm /tmp/hadoop.tar.gz* \
    && mkdir -p /etc/hadoop/conf \
    && cp -r /opt/hadoop-$HADOOP_VERSION/etc/hadoop/* /etc/hadoop/conf
    && if [ -e "/etc/hadoop/conf/mapred-site.xml.template" ]; then cp /etc/hadoop/conf/mapred-site.xml.template /etc/hadoop/conf/mapred-site.xml ;fi \
    && mkdir -p /opt/hadoop-$HADOOP_VERSION/logs \
    && mkdir /hadoop-data

ENV HADOOP_PREFIX=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV MULTIHOMED_NETWORK=1
ENV HADOOP_HOME=${HADOOP_PREFIX}
ENV HADOOP_INSTALL=${HADOOP_HOME}

ENV USER=root
ENV PATH $JAVA_HOME/bin:/usr/bin:/bin:$HADOOP_PREFIX/bin/:$PATH

ADD entrypoint.sh /opt/entrypoint/hadoop/entrypoint.sh
RUN chmod a+x /opt/entrypoint/hadoop/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint/hadoop/entrypoint.sh"]