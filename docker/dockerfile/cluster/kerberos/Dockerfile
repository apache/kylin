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

FROM centos:7.3.1611
MAINTAINER kylin

USER root

# install tools
RUN yum -y install lsof wget tar git unzip wget curl net-tools procps perl sed nc which
# install kerberos
RUN yum -y install krb5-server krb5-libs krb5-auth-dialog krb5-workstation

COPY conf/kadm5.acl  /var/kerberos/krb5kdc/kadm5.acl
COPY conf/kdc.conf /var/kerberos/krb5kdc/kdc.conf
COPY conf/krb5.conf /etc/krb5.conf

ADD run_krb.sh /run_krb.sh
RUN chmod a+x /run_krb.sh

CMD ["/run_krb.sh"]