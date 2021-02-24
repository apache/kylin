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

# Docker image for apache kylin, based on the Hadoop image
FROM hadoop2.7-all-in-one-for-kylin4-beta

ENV KYLIN_VERSION 4.0.0-beta
ENV KYLIN_HOME /home/admin/apache-kylin-$KYLIN_VERSION-bin

# Download Kylin
RUN wget https://archive.apache.org/dist/kylin/apache-kylin-$KYLIN_VERSION/apache-kylin-$KYLIN_VERSION-bin.tar.gz \
    && tar -zxvf /home/admin/apache-kylin-$KYLIN_VERSION-bin.tar.gz \
    && rm -f /home/admin/apache-kylin-$KYLIN_VERSION-bin.tar.gz
RUN rm -f $KYLIN_HOME/conf/kylin.properties
COPY conf/kylin/* $KYLIN_HOME/conf/
RUN rm -f $KYLIN_HOME/bin/kylin.sh
COPY conf/bin/kylin.sh $KYLIN_HOME/bin/
RUN chmod +x $KYLIN_HOME/bin/kylin.sh
RUN cp $HIVE_HOME/lib/mysql-connector-java-5.1.24.jar $KYLIN_HOME/lib/
RUN sed -i "s/hbase/java/g" $KYLIN_HOME/bin/set-java-home.sh

COPY ./entrypoint.sh /home/admin/entrypoint.sh
RUN chmod u+x /home/admin/entrypoint.sh

ENTRYPOINT ["/home/admin/entrypoint.sh"]
