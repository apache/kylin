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

ARG MYSQL_VERSION=5.6.49
FROM mysql:${MYSQL_VERSION}

ARG CREATE_DBS="kylin hive"
ENV CREATE_DBS=$CREATE_DBS

COPY run_db.sh /run_db.sh
RUN chmod +x /run_db.sh

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["/run_db.sh"]
