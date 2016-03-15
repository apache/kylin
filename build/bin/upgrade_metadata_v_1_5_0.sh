#!/bin/bash

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

# the script will upgrade Kylin 1.0 ~ 1.3 compatible metadata store to 1.5 compatible metadata store
# the approach is 1) upgrade to 1.4 compatible 2) upgrade from 1.4 compatible to 1.5 compatible

dir=$(dirname ${0})
source ${dir}/check-env.sh

# start command
if [ "$#" -ne 1 ]
then
    echo "usage: upgrade_metadata_v_1_5_0.sh current_metadata_store_dump_path"
    exit 1
fi


echo "=====Upgrade Cube metadata to 1.4 compatible ====="
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.cube.upgrade.v1_4_0.CubeMetadataUpgrade_v_1_4_0 $1

echo "=====Upgrade Cube metadata to 1.5 compatible ====="
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.cube.upgrade.V1_5_0.CubeMetadataUpgrade_v_1_5_0 $1

echo "=====Refresh all cubes' signature ====="
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.cube.cli.CubeSignatureRefresher

echo "======Deploy coprocessor======="
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI $KYLIN_HOME/lib/kylin-coprocessor-2.0-SNAPSHOT.jar all

echo "==============End=============="
