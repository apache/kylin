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
ARG HBASE_VERSION=1.1.2
ARG HBASE_URL=https://archive.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz

ENV JAVA_HOME /opt/${JAVA_VERSION}
ENV HBASE_VERSION ${HBASE_VERSION}
ENV HBASE_URL ${HBASE_URL}

# install tools
RUN yum -y install lsof wget tar git unzip wget curl net-tools procps perl sed nc which

# setup jdk
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.tar.gz" -P /opt \
    && tar -zxvf /opt/jdk-8u141-linux-x64.tar.gz -C /opt/ \
    && rm -f /opt/jdk-8u141-linux-x64.tar.gz

RUN set -x \
    && curl -fSL "$HBASE_URL" -o /tmp/hbase.tar.gz \
    && curl -fSL "$HBASE_URL.asc" -o /tmp/hbase.tar.gz.asc \
    && tar -xvf /tmp/hbase.tar.gz -C /opt/ \
    && rm /tmp/hbase.tar.gz*

RUN mkdir -p /etc/hbase/conf \
    && cp -r /opt/hbase-$HBASE_VERSION/conf/* /etc/hbase/conf \
    && mkdir /opt/hbase-$HBASE_VERSION/logs

RUN mkdir /hadoop-data

ENV HBASE_PREFIX=/opt/hbase-$HBASE_VERSION
ENV HBASE_HOME=${HBASE_PREFIX}
ENV HBASE_CONF_DIR=/etc/hbase/conf

ENV USER=root
ENV PATH $JAVA_HOME/bin:$HBASE_PREFIX/bin/:$PATH

ADD entrypoint.sh /opt/entrypoint/hbase/entrypoint.sh
RUN chmod a+x /opt/entrypoint/hbase/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint/hbase/entrypoint.sh"]
