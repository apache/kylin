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

source ${KYLIN_HOME:-"$(cd -P -- "$(dirname -- "$0")" && pwd -P)/../"}/bin/header.sh

# source me
hive_conf_dir="${KYLIN_HOME}/conf/kylin_hive_conf.xml"

names=(`sed -n '/<!--/,/-->/!p' ${hive_conf_dir} | sed -n 's|<name>\(.*\)</name>|\1|p' | sed 's/ \+//g'`)
values=(`sed -n '/<!--/,/-->/!p' ${hive_conf_dir} | sed -n 's|<value>\(.*\)</value>|\1|p' | sed 's/ \+//g'`)

len_names=${#names[@]}
len_values=${#values[@]}

[[ $len_names == $len_values ]] || quit "Current hive conf file: ${hive_conf_dir} has inconsistent key value pairs."

for((i=0;i<$len_names;i++))
do
    hive_conf_properties=${hive_conf_properties}" --hiveconf ${names[$i]}=${values[$i]} "
done
