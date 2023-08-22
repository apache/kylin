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

# This is the Docker Image for Kylin 5 release process
#
# For what is release policy and release process, please refer to following link:
# 1. https://www.apache.org/legal/release-policy.html
# 2. https://infra.apache.org/release-publishing.html

FROM openjdk:8-slim

ENV M2_HOME /root/apache-maven-3.8.8
ENV PATH $PATH:$M2_HOME/bin
USER root

WORKDIR /root

# install tools
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends lsof wget tar

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends git unzip subversion

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends curl vim ssh

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends gcc g++ make bzip2

# install Node JS
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash - \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs

# install maven
RUN wget https://archive.apache.org/dist/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz \
    && tar -zxvf apache-maven-3.8.8-bin.tar.gz \
    && rm -f apache-maven-3.8.8-bin.tar.gz

RUN apt-get install -y --fix-missing ruby-full

RUN gem install jekyll:2.5.3 jekyll-multiple-languages:1.0.11  rouge:1.10.1 kramdown:1.17.0

# install Node JS(this new version is for kylin website)
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.4/install.sh | bash

ENV NVM_DIR="/root/.nvm"

RUN mkdir /root/.m2
COPY conf/settings.xml /root/.m2/settings.xml

COPY script/entrypoint.sh /root/scripts/entrypoint.sh
RUN chmod u+x /root/scripts/entrypoint.sh

COPY script/release-publish.sh /root/release-publish.sh
RUN chmod u+x /root/release-publish.sh

#ENTRYPOINT ["/root/scripts/entrypoint.sh"]