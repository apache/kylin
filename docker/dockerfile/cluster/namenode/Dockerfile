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

ENV HADOOP_VERSION ${HADOOP_VERSION}

ARG HADOOP_WEBHDFS_PORT=50070
ENV HADOOP_WEBHDFS_PORT ${HADOOP_WEBHDFS_PORT}
EXPOSE ${HADOOP_WEBHDFS_PORT} 8020

HEALTHCHECK CMD curl -f http://localhost:${HADOOP_WEBHDFS_PORT}/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run_nn.sh /run_nn.sh
RUN chmod a+x /run_nn.sh

CMD ["/run_nn.sh"]
