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
ADD hdp-clients/hadoop-${HADOOP_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/hadoop-${HADOOP_VERSION} $HADOOP_HOME

#install hive
ENV HIVE_HOME=$APACHE_HOME/hive 
ADD hdp-clients/apache-hive-${HIVE_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/apache-hive-${HIVE_VERSION} $HIVE_HOME

#install hbase
ENV HBASE_HOME=$APACHE_HOME/hbase
ADD hdp-clients/hbase-${HBASE_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/hbase-${HBASE_VERSION} $HBASE_HOME

#install spark
ENV SPARK_HOME=$APACHE_HOME/spark
ADD hdp-clients/spark-${SPARK_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/spark-${SPARK_VERSION} $SPARK_HOME

#install zk
ENV ZK_HOME=$APACHE_HOME/zookeeper
ADD hdp-clients/zookeeper-${ZK_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/zookeeper-${ZK_VERSION} $ZK_HOME

ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$HBASE_HOME/bin:$ZK_HOME/bin

COPY conf/hadoop $HADOOP_HOME/etc/hadoop
COPY conf/hbase $HBASE_HOME/conf
COPY conf/hive $HIVE_HOME/conf
COPY conf/krb5.conf /etc/krb5.conf

# Cleanup
RUN rm -rf /tmp/*
