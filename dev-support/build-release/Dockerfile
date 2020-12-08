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

# Docker image for Kylin release
FROM openjdk:8-slim

ENV M2_HOME /root/apache-maven-3.6.1
ENV PATH $PATH:$M2_HOME/bin
USER root

WORKDIR /root

# install tools
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends lsof wget tar git unzip subversion

# install maven
RUN wget https://archive.apache.org/dist/maven/maven-3/3.6.1/binaries/apache-maven-3.6.1-bin.tar.gz \
    && tar -zxvf apache-maven-3.6.1-bin.tar.gz \
    && rm -f apache-maven-3.6.1-bin.tar.gz
COPY conf/settings.xml $MVN_HOME/conf/settings.xml

# install tomcat
RUN wget https://archive.apache.org/dist/tomcat/tomcat-7/v7.0.100/bin/apache-tomcat-7.0.100.tar.gz

# install npm
RUN apt-get install -y curl
RUN curl -sL https://deb.nodesource.com/setup_8.x | bash - \
    && apt-get update \
    && apt-get install -y nodejs npm


COPY script/entrypoint.sh /root/entrypoint.sh
RUN chmod u+x /root/entrypoint.sh

COPY script/build_release.sh /root/build_release.sh
RUN chmod u+x /root/build_release.sh

ENTRYPOINT ["/root/entrypoint.sh"]