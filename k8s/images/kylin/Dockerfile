ARG HADOOP_CLIENT_VERSION=3.0.0

FROM apachekylin/hadoop-client:$HADOOP_CLIENT_VERSION

MAINTAINER Apache Kylin

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

ARG KYLIN_VERSION=3.0.0
ARG USER=apache_kylin

ARG USER_HOME=/home/$USER
RUN set -x \
    && groupadd -r $USER \
    && useradd -r -m -g $USER $USER -d $USER_HOME \
    && echo '$USER ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

ENV KYLIN_HOME=$USER_HOME/kylin


COPY --chown=$USER:$USER apache-kylin-${KYLIN_VERSION}-bin $KYLIN_HOME

ARG HADOOP_CONF_HOME=/apache/hadoop/etc/hadoop
ARG HIVE_CONF_HOME=/apache/hive/conf
ARG HBASE_CONF_HOME=/apache/hbase/conf
ARG SPARK_CONF_HOME=$KYLIN_HOME/hadoop-conf

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

CMD ["sh", "-c", "$TOOL_HOME/bootstrap.sh server -d"]
