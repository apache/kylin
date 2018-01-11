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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

## ${dir} assigned to $KYLIN_HOME/bin in header.sh
source ${dir}/load-hive-conf.sh

echo Retrieving hive dependency...

client_mode=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.client`
hive_env=

if [ "${client_mode}" == "beeline" ]
then
    beeline_shell=`$KYLIN_HOME/bin/get-properties.sh kylin.source.hive.beeline-shell`
    beeline_params=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.beeline-params`
    hive_env=`${beeline_shell} ${hive_conf_properties} ${beeline_params} --outputformat=dsv -e set 2>&1 | grep 'env:CLASSPATH' `
else
    hive_env=`hive ${hive_conf_properties} -e set 2>&1 | grep 'env:CLASSPATH'`
fi

hive_classpath=`echo $hive_env | grep 'env:CLASSPATH' | awk -F '=' '{print $2}'`
arr=(`echo $hive_classpath | cut -d ":" -f 1- | sed 's/:/ /g'`)
hive_conf_path=
hive_exec_path=

if [ -n "$HIVE_CONF" ]
then
    verbose "HIVE_CONF is set to: $HIVE_CONF, use it to locate hive configurations."
    hive_conf_path=$HIVE_CONF
fi

for data in ${arr[@]}
do
    result=`echo $data | grep -e 'hive-exec[a-z0-9A-Z\.-]*.jar'`
    # In some cases there are more than one lib dirs, only the first one will be applied.
    if [ $result ] && [ -z "$hive_exec_path" ]
    then
        hive_exec_path=$data
    fi

    # in some versions of hive config is not in hive's classpath, find it separately
    if [ -z "$hive_conf_path" ]
    then
        result=`echo $data | grep -e 'hive[^/]*/conf'`
        if [ $result ]
        then
            hive_conf_path=$data
        fi
    fi
done

if [ -z "$hive_conf_path" ]
then
    quit "Couldn't find hive configuration directory. Please set HIVE_CONF to the path which contains hive-site.xml."
fi

# in some versions of hive hcatalog is not in hive's classpath, find it separately
if [ -z "$HCAT_HOME" ]
then
    verbose "HCAT_HOME not found, try to find hcatalog path from hadoop home"
    hadoop_home=`echo $hive_exec_path | awk -F '/hive.*/lib/hive-exec[a-z0-9A-Z.-]*.jar' '{print $1}'`
    hive_home=`echo $hive_exec_path | awk -F '/lib/hive-exec[a-z0-9A-Z.-]*.jar' '{print $1}'`
    is_aws=`uname -r | grep amzn`
    if [ -d "${hadoop_home}/hive-hcatalog" ]; then
      hcatalog_home=${hadoop_home}/hive-hcatalog
    elif [ -d "${hadoop_home}/hive/hcatalog" ]; then
      hcatalog_home=${hadoop_home}/hive/hcatalog
    elif [ -d "${hive_home}/hcatalog" ]; then
      hcatalog_home=${hive_home}/hcatalog
    elif [ -n is_aws ] && [ -d "/usr/lib/hive-hcatalog" ]; then
      # special handling for Amazon EMR
      hcatalog_home=/usr/lib/hive-hcatalog
    else 
      quit "Couldn't locate hcatalog installation, please make sure it is installed and set HCAT_HOME to the path."
    fi
else
    verbose "HCAT_HOME is set to: $HCAT_HOME, use it to find hcatalog path:"
    hcatalog_home=${HCAT_HOME}
fi

hcatalog=`find -L ${hcatalog_home} -name "hive-hcatalog-core[0-9\.-]*.jar" 2>&1 | grep -m 1 -v 'Permission denied'`

if [ -z "$hcatalog" ]
then
    quit "hcatalog lib not found"
fi

function checkFileExist()
{
    files=(`echo $1 | cut -d ":" -f 1- | sed 's/:/ /g'`)
    misFiles=0
    outputMissFiles=
    for file in ${files}
    do
        let allFiles++
        if [ ! -f "${file}" ]; then
            outputMissFiles=${outputMissFiles}${file}", "
            let misFiles++
        fi
    done
    if [ 0 != ${misFiles} ]; then
        times=`expr ${allFiles} / ${misFiles}`
        [[ ${times} -gt 10 ]] || quit "A couple of hive jars can't be found: ${outputMisFiles}, please export HIVE_LIB='YOUR_LOCAL_HIVE_LIB'"
    fi
}

function validateDirectory()
{
    conf_path=$1
    [[ -d "${conf_path}" ]] || quit "${conf_path} doesn't exist!"
    unit=${conf_path: -1}
    [[ "${unit}" == "/" ]] || conf_path=${conf_path}"/"

    find="false"
    filelist=`ls ${conf_path}`
    for file in $filelist
    do
        if [ "${file}" == "hive-site.xml" ]
        then
            find="true"
            break
        fi
    done
    [[ "${find}" == "true" ]] || quit "ERROR, no hive-site.xml found under dir: ${conf_path}!"
}

if [ -z "$HIVE_LIB" ]
then
    verbose "HIVE_LIB is not set, try to retrieve hive lib from hive_exec_path"
    hive_lib_dir="$(dirname $hive_exec_path)"
else
    hive_lib_dir="$HIVE_LIB"
fi
hive_lib=`find -L ${hive_lib_dir} -name '*.jar' ! -name '*calcite*' ! -name '*jackson-datatype-joda*' -printf '%p:' | sed 's/:$//'`

validateDirectory ${hive_conf_path}
checkFileExist ${hive_lib}
checkFileExist ${hcatalog}

hive_dependency=${hive_conf_path}:${hive_lib}:${hcatalog}
verbose "hive dependency is $hive_dependency"
export hive_dependency
export hive_conf_path
