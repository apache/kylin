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

ARG JAVA_VERSION=jdk1.8.0_141
ARG HADOOP_VERSION=2.8.5
ARG HIVE_VERSION=1.2.1
ARG HBASE_VERSION=1.1.2
ARG ZOOKEEPER_VERSION=3.4.10
ARG KAFKA_VERSION=2.0.0
ARG SPARK_VERSION=2.3.1
ARG SPARK_HADOOP_VERSION=2.6

FROM apachekylin/kylin-ci-hive:hive_${HIVE_VERSION}_hadoop_${HADOOP_VERSION} AS hive
ENV JAVA_VERSION ${JAVA_VERSION}
ENV HADOOP_VERSION ${HADOOP_VERSION}
ENV HIVE_VERSION ${HIVE_VERSION}

ARG HBASE_VERSION=1.1.2
FROM apachekylin/kylin-ci-hbase-master:hbase_${HBASE_VERSION} AS hbase
ENV HBASE_VERSION ${HBASE_VERSION}


ARG ZOOKEEPER_VERSION=3.4.10
FROM zookeeper:${ZOOKEEPER_VERSION} AS zk
ENV ZOOKEEPER_VERSION ${ZOOKEEPER_VERSION}

ARG KAFKA_VERSION=2.0.0
FROM bitnami/kafka:${KAFKA_VERSION} AS kafka
ENV KAFKA_VERSION ${KAFKA_VERSION}

FROM centos:7.3.1611
MAINTAINER kylin
USER root

ARG JAVA_VERSION=jdk1.8.0_141
ARG HADOOP_VERSION=2.8.5
ARG HIVE_VERSION=1.2.1
ARG HBASE_VERSION=1.1.2
ARG ZOOKEEPER_VERSION=3.4.10
ARG KAFKA_VERSION=2.0.0
ARG SPARK_VERSION=2.4.7
ARG SPARK_HADOOP_VERSION=2.7

ENV JAVA_VERSION ${JAVA_VERSION}
ENV HADOOP_VERSION ${HADOOP_VERSION}
ENV HIVE_VERSION ${HIVE_VERSION}
ENV HBASE_VERSION ${HBASE_VERSION}
ENV ZOOKEEPER_VERSION ${ZOOKEEPER_VERSION}
ENV KAFKA_VERSION ${KAFKA_VERSION}
ENV SPARK_VERSION ${SPARK_VERSION}
ENV SPARK_HADOOP_VERSION ${SPARK_HADOOP_VERSION}

## install tools
RUN yum -y install lsof wget tar git unzip wget curl net-tools procps perl sed nc which
# install kerberos
RUN yum -y install krb5-server krb5-libs krb5-auth-dialog krb5-workstation

RUN mkdir /opt/hadoop-$HADOOP_VERSION/

COPY --from=hive /opt/jdk1.8.0_141/ /opt/jdk1.8.0_141/
COPY --from=hive /opt/hadoop-$HADOOP_VERSION/ /opt/hadoop-$HADOOP_VERSION/
COPY --from=hive /opt/hive/ /opt/hive/
COPY --from=hive /opt/entrypoint/hadoop/entrypoint.sh /opt/entrypoint/hadoop/entrypoint.sh
RUN chmod a+x /opt/entrypoint/hadoop/entrypoint.sh
COPY --from=hive /opt/entrypoint/hive/entrypoint.sh /opt/entrypoint/hive/entrypoint.sh
RUN chmod a+x /opt/entrypoint/hive/entrypoint.sh


COPY --from=hbase /opt/hbase-$HBASE_VERSION/ /opt/hbase-$HBASE_VERSION/
COPY --from=hbase /opt/entrypoint/hbase/entrypoint.sh /opt/entrypoint/hbase/entrypoint.sh
RUN chmod a+x /opt/entrypoint/hbase/entrypoint.sh


COPY --from=zk /zookeeper-${ZOOKEEPER_VERSION}/ /opt/zookeeper-${ZOOKEEPER_VERSION}/
COPY --from=zk /docker-entrypoint.sh /opt/entrypoint/zookeeper/entrypoint.sh
RUN chmod a+x /opt/entrypoint/zookeeper/entrypoint.sh

COPY --from=kafka /opt/bitnami/kafka /opt/kafka
COPY --from=kafka /app-entrypoint.sh /opt/entrypoint/kafka/entrypoint.sh
RUN chmod a+x /opt/entrypoint/kafka/entrypoint.sh


RUN set -x \
    && mkdir -p /etc/hadoop/conf \
    && mkdir -p /etc/hbase/conf \
    && cp -r /opt/hadoop-$HADOOP_VERSION/etc/hadoop/* /etc/hadoop/conf \
    && cp -r /opt/hbase-$HBASE_VERSION/conf/* /etc/hbase/conf \
    && if [ -e "/etc/hadoop/conf/mapred-site.xml.template" ]; then cp /etc/hadoop/conf/mapred-site.xml.template /etc/hadoop/conf/mapred-site.xml ;fi \
    && mkdir -p /opt/hadoop-$HADOOP_VERSION/logs

ENV JAVA_HOME=/opt/${JAVA_VERSION}

ENV HADOOP_PREFIX=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV HADOOP_HOME=${HADOOP_PREFIX}
ENV HADOOP_INSTALL=${HADOOP_HOME}

ENV HIVE_HOME=/opt/hive

ENV HBASE_PREFIX=/opt/hbase-$HBASE_VERSION
ENV HBASE_CONF_DIR=/etc/hbase/conf
ENV HBASE_HOME=${HBASE_PREFIX}


ENV ZK_HOME=/opt/zookeeper-${ZOOKEEPER_VERSION}
ENV ZOOCFGDIR=$ZK_HOME/conf
ENV ZOO_USER=zookeeper
ENV ZOO_CONF_DIR=$ZK_HOME/conf ZOO_PORT=2181 ZOO_TICK_TIME=2000 ZOO_INIT_LIMIT=5 ZOO_SYNC_LIMIT=2 ZOO_MAX_CLIENT_CNXNS=60

ENV SPARK_URL=https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop${SPARK_HADOOP_VERSION}.tgz
ENV SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop${SPARK_HADOOP_VERSION}
ENV SPARK_CONF_DIR=/opt/spark-$SPARK_VERSION-bin-hadoop${SPARK_HADOOP_VERSION}/conf

RUN curl -fSL "${SPARK_URL}" -o /tmp/spark.tar.gz \
    && tar -zxvf /tmp/spark.tar.gz -C /opt/ \
    && rm -f /tmp/spark.tar.gz \
    && rm -f $SPARK_HOME/conf/hive-site.xml \
    && ln -s $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml \
    && cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf \
    && cp $SPARK_HOME/yarn/*.jar $HADOOP_HOME/share/hadoop/yarn/lib

#COPY spark-$SPARK_VERSION-bin-hadoop${SPARK_HADOOP_VERSION}.tgz /tmp/spark.tar.gz
#RUN tar -zxvf /tmp/spark.tar.gz -C /opt/ \
#    && rm -f /tmp/spark.tar.gz \
#    && cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf \
#    && cp $SPARK_HOME/yarn/*.jar $HADOOP_HOME/share/hadoop/yarn/lib

#RUN cp $HIVE_HOME/lib/mysql-connector-java.jar $SPARK_HOME/jars
RUN cp $HIVE_HOME/lib/postgresql-jdbc.jar  $SPARK_HOME/jars
RUN cp $HBASE_HOME/lib/hbase-protocol-${HBASE_VERSION}.jar $SPARK_HOME/jars
RUN echo spark.sql.catalogImplementation=hive > $SPARK_HOME/conf/spark-defaults.conf


ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$HBASE_HOME/bin:$ZK_HOME/bin

# Hadoop Client Configuration
COPY entrypoint.sh /opt/entrypoint/client/entrypoint.sh
RUN chmod a+x /opt/entrypoint/client/entrypoint.sh

RUN rm -f /opt/hbase-${HBASE_VERSION}/lib/hadoop-*.jar

COPY run_cli.sh /run_cli.sh
RUN chmod a+x  /run_cli.sh

#ENTRYPOINT ["/opt/entrypoint/client/entrypoint.sh"]

CMD ["/run_cli.sh"]
