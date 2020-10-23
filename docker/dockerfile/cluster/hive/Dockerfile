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

ARG HADOOP_VERSION=2.8.5
FROM apachekylin/kylin-ci-hadoop-base:hadoop_${HADOOP_VERSION}

ENV HIVE_HOME /opt/hive
ENV HADOOP_HOME /opt/hadoop-$HADOOP_VERSION

WORKDIR /opt

ARG HIVE_VERSION=1.2.1
ARG HIVE_URL=https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz
ENV HIVE_VERSION ${HIVE_VERSION}
ENV HIVE_URL ${HIVE_URL}

ARG MYSQL_CONN_VERSION=8.0.20
ENV MYSQL_CONN_VERSION=${MYSQL_CONN_VERSION}
ARG MYSQL_CONN_URL=https://downloads.mysql.com/archives/get/p/3/file/mysql-connector-java-${MYSQL_CONN_VERSION}.tar.gz
ENV MYSQL_CONN_URL=${MYSQL_CONN_URL}

# install tools
RUN yum -y install lsof wget tar git unzip wget curl net-tools procps perl sed nc which

#Install Hive MySQL, PostgreSQL JDBC
RUN echo "Hive URL is :${HIVE_URL}" \
    && wget ${HIVE_URL} -O hive.tar.gz \
    && tar -xzvf hive.tar.gz \
    && mv *hive*-bin hive \
    && wget $MYSQL_CONN_URL -O /tmp/mysql-connector-java.tar.gz \
    && tar -xzvf /tmp/mysql-connector-java.tar.gz -C /tmp/ \
    && cp /tmp/mysql-connector-java-${MYSQL_CONN_VERSION}/mysql-connector-java-${MYSQL_CONN_VERSION}.jar $HIVE_HOME/lib/mysql-connector-java.jar \
    && rm /tmp/mysql-connector-java.tar.gz \
    && rm -rf /tmp/mysql-connector-java-${MYSQL_CONN_VERSION} \
    && wget https://jdbc.postgresql.org/download/postgresql-9.4.1212.jar -O $HIVE_HOME/lib/postgresql-jdbc.jar \
    && rm hive.tar.gz

RUN if [[ $HADOOP_VERSION > "3" ]]; then rm -rf $HIVE_HOME/lib/guava-* ; cp $HADOOP_HOME/share/hadoop/common/lib/guava-* $HIVE_HOME/lib; fi

#Custom configuration goes here
ADD conf/hive-site.xml $HIVE_HOME/conf
ADD conf/beeline-log4j2.properties $HIVE_HOME/conf
ADD conf/hive-env.sh $HIVE_HOME/conf
ADD conf/hive-exec-log4j2.properties $HIVE_HOME/conf
ADD conf/hive-log4j2.properties $HIVE_HOME/conf
ADD conf/ivysettings.xml $HIVE_HOME/conf
ADD conf/llap-daemon-log4j2.properties $HIVE_HOME/conf

COPY run_hv.sh /run_hv.sh
RUN chmod +x /run_hv.sh

COPY entrypoint.sh /opt/entrypoint/hive/entrypoint.sh
RUN chmod +x /opt/entrypoint/hive/entrypoint.sh

ENV PATH $HIVE_HOME/bin/:$PATH

EXPOSE 10000
EXPOSE 10002

ENTRYPOINT ["/opt/entrypoint/hive/entrypoint.sh"]
CMD ["/run_hv.sh"]
