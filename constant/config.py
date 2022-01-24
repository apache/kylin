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

from enum import Enum


class Config(Enum):
    # DEBUG params
    CLOUD_ADDR = 'CLOUD_ADDR'

    # Other params
    AWS_REGION = 'AWS_REGION'
    KYLIN_SCALE_UP_NODES = 'KYLIN_SCALE_UP_NODES'
    KYLIN_SCALE_DOWN_NODES = 'KYLIN_SCALE_DOWN_NODES'
    SPARK_WORKER_SCALE_UP_NODES = 'SPARK_WORKER_SCALE_UP_NODES'
    SPARK_WORKER_SCALE_DOWN_NODES = 'SPARK_WORKER_SCALE_DOWN_NODES'
    ENABLE_SOFT_AFFINITY = 'ENABLE_LOCAL_CACHE_SOFT_AFFINITY'
    IAM = 'IAMRole'
    KEY_PAIR = 'KeyName'
    CLUSTER_INDEXES = 'CLUSTER_INDEXES'
    CIDR_IP = 'CIDR_IP'

    # Deploy params
    DEPLOY_PLATFORM = 'DEPLOY_PLATFORM'

    # Stack Names
    VPC_STACK = 'VPC_STACK'
    RDS_STACK = 'RDS_STACK'
    STATIC_SERVICE_STACK = 'STATIC_SERVICE_STACK'
    ZOOKEEPERS_STACK = 'ZOOKEEPERS_STACK'
    KYLIN_STACK = 'KYLIN_STACK'
    SPARK_MASTER_STACK = 'SPARK_MASTER_STACK'
    SPARK_WORKER_STACK = 'SPARK_WORKER_STACK'

    # Params
    EC2_STATIC_SERVICES_PARAMS = 'EC2_STATIC_SERVICES_PARAMS'
    EC2_KYLIN4_PARAMS = 'EC2_KYLIN4_PARAMS'
    EC2_KYLIN4_SCALE_PARAMS = 'EC2_KYLIN4_SCALE_PARAMS'
    EC2_VPC_PARAMS = 'EC2_VPC_PARAMS'
    EC2_RDS_PARAMS = 'EC2_RDS_PARAMS'
    EC2_ZOOKEEPERS_PARAMS = 'EC2_ZOOKEEPERS_PARAMS'
    EC2_SPARK_MASTER_PARAMS = 'EC2_SPARK_MASTER_PARAMS'
    EC2_SPARK_WORKER_PARAMS = 'EC2_SPARK_WORKER_PARAMS'
    EC2_SPARK_SCALE_SLAVE_PARAMS = 'EC2_SPARK_SCALE_SLAVE_PARAMS'

    # RDS Params
    DB_IDENTIFIER = 'DB_IDENTIFIER'

    DB_PORT = 'DB_PORT'
    DB_USER = 'DB_USER'
    DB_PASSWORD = 'DB_PASSWORD'
