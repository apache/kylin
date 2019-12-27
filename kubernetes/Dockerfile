FROM centos:6.9

ARG APACHE_MIRRORS=http://mirrors.aliyun.com
ENV APACHE_MIRRORS  ${APACHE_MIRRORS}

ENV JAVA_VERSION    1.8.0
ENV SPARK_VERSION   2.3.4
ENV KAFKA_VERSION   2.1.1
ENV KYLIN_VERSION   3.0.0

ENV JAVA_HOME       /usr/lib/jvm/java-${JAVA_VERSION}
ENV HADOOP_HOME     /usr/lib/hadoop
ENV HIVE_HOME       /usr/lib/hive
ENV HCAT_HOME       /usr/lib/hive-hcatalog
ENV HBASE_HOME      /usr/lib/hbase
ENV SPARK_HOME      /opt/spark-${SPARK_VERSION}-bin-hadoop2.6
ENV KAFKA_HOME      /opt/kafka_2.11-${KAFKA_VERSION}
ENV KYLIN_HOME      /opt/apache-kylin-${KYLIN_VERSION}-bin-cdh57

ENV PATH $PATH:\
$SPARK_HOME/bin:\
$KAFKA_HOME/bin:\
$KYLIN_HOME/bin

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

# Download Kafka from APACHE_MIRRORS
RUN set -xeu && \
    curl -o kafka_2.11-${KAFKA_VERSION}.tgz \
    ${APACHE_MIRRORS}/apache/kafka/${KAFKA_VERSION}/kafka_2.11-${KAFKA_VERSION}.tgz && \
    tar -zxf kafka_2.11-${KAFKA_VERSION}.tgz && rm kafka_2.11-${KAFKA_VERSION}.tgz

# Download Spark from APACHE_MIRRORS
RUN set -xeu && \
    curl -o spark-${SPARK_VERSION}-bin-hadoop2.6.tgz \
    ${APACHE_MIRRORS}/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.6.tgz && \
    tar -zxf spark-${SPARK_VERSION}-bin-hadoop2.6.tgz && rm spark-${SPARK_VERSION}-bin-hadoop2.6.tgz

# Download Kylin from APACHE_MIRRORS
RUN set -xeu && \
    curl -o apache-kylin-${KYLIN_VERSION}-bin-cdh57.tar.gz \
    ${APACHE_MIRRORS}/apache/kylin/apache-kylin-${KYLIN_VERSION}/apache-kylin-${KYLIN_VERSION}-bin-cdh57.tar.gz && \
    tar -zxf apache-kylin-${KYLIN_VERSION}-bin-cdh57.tar.gz && rm apache-kylin-${KYLIN_VERSION}-bin-cdh57.tar.gz

# Setup Hadoop & Hive & HBase using CDH Repository. PS: The libhadoop.so provided by CDH is complied with snappy
RUN set -xeu && \
    yum -y -q install java-1.8.0-openjdk-devel && \
    yum -y -q install krb5-workstation && \
    yum -y -q install hadoop-client && \
    yum -y -q install hive hive-hcatalog && \
    yum -y -q install hbase && \
    curl -o ${HIVE_HOME}/lib/hadoop-lzo-0.4.15.jar \
    https://clojars.org/repo/hadoop-lzo/hadoop-lzo/0.4.15/hadoop-lzo-0.4.15.jar && \
    curl -o ${HIVE_HOME}/lib/mysql-connector-java-5.1.24.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.24/mysql-connector-java-5.1.24.jar && \
    yum -q clean all && \
    rm -rf /var/cache/yum && \
    rm -rf /tmp/* /var/tmp/* && \
    groupadd kylin --gid 1000 && \
    useradd kylin --uid 1000 --gid 1000 && \
    chown -R "kylin:kylin" ${KYLIN_HOME}

EXPOSE 7070
USER kylin:kylin
CMD ${KYLIN_HOME}/bin/kylin.sh run