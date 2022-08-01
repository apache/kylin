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

function help() {
    echo "Usage: upgrade.sh <OLD_KYLIN_HOME> [--silent]"
    echo
    echo "<OLD_KYLIN_HOME>    Specify the old version of the Kyligence Enterprise"
    echo "                    installation directory."
    echo
    echo "--silent            Optional, don't enter interactive mode, automatically complete the upgrade."
    exit 1
}

function info() {
    echo -e "\033[32m$@\033[0m"
}

function warn() {
    echo -e "\033[33m$@\033[0m"
}

function error() {
    echo -e "\033[31m$@\033[0m"
}

function logging() {
    case $1 in
        "info") shift; info $@ ;;
        "warn") shift; warn $@ ;;
        "error") shift; error $@ ;;
        *) echo -e $@ ;;
    esac

    (echo -e `date '+%F %H:%M:%S'` $@ >> $upgrade_log)
}

function fail() {
    error "...................................................[FAIL]"
    error "Upgrade Kyligence Enterprise failed."
    recordKylinUpgradeResult "${START_TIME}" "false" "${NEW_KYLIN_HOME}"
    exit 1
}

function prompt() {
    if [[ $silent -eq 0 ]]; then
        return 0
    fi

    read -p "$@ (y/n) > " answer
    if [[ -z $answer ]] || [[ $answer == "y" ]]; then
        return 0
    else
        return 1
    fi
}

function check_kylin_query_transformers() {
    query_transformers=""
    if [[ -f ${OLD_KYLIN_HOME}/conf/kylin.properties.override ]]; then
        query_transformers=$(sed -n '/^kylin.query.transformers/p' ${OLD_KYLIN_HOME}/conf/kylin.properties.override)
    fi

    if [[ -z "${query_transformers}" && -f ${OLD_KYLIN_HOME}/conf/kylin.properties ]]; then
        query_transformers=$(sed -n '/^kylin.query.transformers/p' ${OLD_KYLIN_HOME}/conf/kylin.properties)
    fi

    if [[ -n "${query_transformers}" && (! ${query_transformers} =~ io.kyligence.kap.query.security.RowFilter) ]]; then
          error "Please check the value of the configuration item [kylin.query.transformers] in kylin.properties or kylin.properties.override, which needs to include [org.apache.kylin.query.security.RowFilter] class."
          exit 1
    fi
}

function upgrade() {

    check_kylin_query_transformers

    # needed by km
    if [[ -f ${OLD_KYLIN_HOME}/pid ]]; then
        PID=`cat ${OLD_KYLIN_HOME}/pid`
        if ps -p $PID > /dev/null; then
          error "Please stop the Kyligence Enterprise during the upgrade process."
          exit 1
        fi
    fi

    if [[ -f ${OLD_KYLIN_HOME}/grafana/pid ]]; then
        PID=`cat ${OLD_KYLIN_HOME}/grafana/pid`
        if ps -p $PID > /dev/null; then
          error "Please stop the Grafana during the upgrade process."
          exit 1
        fi
    fi

    echo `date '+%Y-%m-%d %H:%M:%S '`"INFO : [Operation: upgrade] user:`whoami`, upgrade time:${START_TIME}" >> ${NEW_KYLIN_HOME}/logs/security.log
    origin_version=$(awk '{print $NF}' ${OLD_KYLIN_HOME}/VERSION)
    target_version=$(awk '{print $NF}' ${NEW_KYLIN_HOME}/VERSION)
    echo
    logging "warn" "Upgrade Kyligence Enterprise from ${origin_version} to ${target_version}"
    warn "Old KYLIN_HOME is ${OLD_KYLIN_HOME}, log is at ${upgrade_log}"
    echo

    # copy LICENSE
    logging "Copy LICENSE"
    if [[ -f ${OLD_KYLIN_HOME}/LICENSE ]]; then
        if prompt "'${OLD_KYLIN_HOME}/LICENSE' -> '${NEW_KYLIN_HOME}/'"; then
            \cp -vf ${OLD_KYLIN_HOME}/LICENSE ${NEW_KYLIN_HOME}/ >> $upgrade_log || fail
        fi
    fi
    info "...................................................[DONE]"

    # copy kylin conf
    # exclude 'profile*' directory
    logging "Copy Kylin Conf"
    for conf_file in $(ls $OLD_KYLIN_HOME/conf); do
        if prompt "'${OLD_KYLIN_HOME}/conf/${conf_file}' -> '${NEW_KYLIN_HOME}/conf/'"; then
            if [[ -d ${OLD_KYLIN_HOME}/conf/${conf_file} ]]; then
                # silent copy directory
                \cp -rfv ${OLD_KYLIN_HOME}/conf/${conf_file} ${NEW_KYLIN_HOME}/conf/ >> $upgrade_log || fail
            else
                # need to delete the symbolic link first
                \cp -vf --remove-destination ${OLD_KYLIN_HOME}/conf/${conf_file} ${NEW_KYLIN_HOME}/conf/ >> $upgrade_log || fail
            fi

        fi
    done
    info "...................................................[DONE]"

    # copy ext jars
    # copy ext/mysql*.jar to spark/jars
    logging "Copy Ext Jars"
    for jar_file in $(ls $OLD_KYLIN_HOME/lib/ext); do
        if prompt "'${OLD_KYLIN_HOME}/lib/ext/${jar_file}' -> '${NEW_KYLIN_HOME}/lib/ext/'"; then
            \cp -vf ${OLD_KYLIN_HOME}/lib/ext/${jar_file} ${NEW_KYLIN_HOME}/lib/ext/ >> $upgrade_log || fail
        fi

        if [[ ${jar_file}} == mysql* ]];
        then
          if prompt "'${OLD_KYLIN_HOME}/lib/ext/${jar_file}' -> '${NEW_KYLIN_HOME}/spark/jars/'"; then
            \cp -vf ${OLD_KYLIN_HOME}/lib/ext/${jar_file} ${NEW_KYLIN_HOME}/spark/jars/ >> $upgrade_log || fail
          fi
        fi
    done
    info "...................................................[DONE]"

    # copy mysql connector jar to spark jars dir for apache hadoop platform
    APACHE_HADOOP_CONF_DIR=`${NEW_KYLIN_HOME}/bin/get-properties.sh kylin.env.apache-hadoop-conf-dir`
    if [ -n "${APACHE_HADOOP_CONF_DIR}" ]; then
      logging "Copy mysql connector jar to spark jars dir for apache hadoop platform"
      \cp -vf ${OLD_KYLIN_HOME}/lib/ext/mysql-connector-*.jar ${NEW_KYLIN_HOME}/spark/jars/ >> $upgrade_log || fail
      info "...................................................[DONE]"
    fi

    # copy the customize directory under old kylin home
    # such as hadoop_conf
    logging "Copy Customize Directory"
    OLDIFS=$IFS
    IFS=$'\n'
    for diff_log in $(diff -qr $OLD_KYLIN_HOME $NEW_KYLIN_HOME); do
        if [[ $diff_log =~ (^Only in ${OLD_KYLIN_HOME}: )(.*) ]]; then
            diff_file=${BASH_REMATCH[2]}
            if [[ $diff_file == "meta_backups" || $diff_file == "appid" || $diff_file == "work" ]]; then
                continue
            fi
            if prompt "'${OLD_KYLIN_HOME}/${diff_file}' -> '${NEW_KYLIN_HOME}/'"; then
                cp -rfv ${OLD_KYLIN_HOME}/${diff_file} ${NEW_KYLIN_HOME}/ >> $upgrade_log || fail
            fi
        fi
    done
    IFS=$OLDIFS
    info "...................................................[DONE]"

    # Ensure krb5.conf underlying hadoop_conf if kerberos enabled
    logging "Copy krb5.conf"
    if [[ -f ${OLD_KYLIN_HOME}/conf/krb5.conf ]]; then
        if [[ -d ${NEW_KYLIN_HOME}/hadoop_conf ]]; then
          cp -rfv ${OLD_KYLIN_HOME}/conf/krb5.conf ${NEW_KYLIN_HOME}/hadoop_conf
        fi

        if [[ -d ${NEW_KYLIN_HOME}/write_hadoop_conf ]]; then
          cp -rfv ${OLD_KYLIN_HOME}/conf/krb5.conf ${NEW_KYLIN_HOME}/write_hadoop_conf
        fi
    fi
    info "...................................................[DONE]"

    logging "Copy hive*-site.xml for spark3"
    if [[ -f ${OLD_KYLIN_HOME}/hadoop_conf/hive-site.xml ]]; then
        if [[ -d ${NEW_KYLIN_HOME}/hadoop_conf ]]; then
          cp -rfv ${OLD_KYLIN_HOME}/hadoop_conf/hive-site.xml ${NEW_KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
          cp -rfv ${OLD_KYLIN_HOME}/hadoop_conf/hive-site.xml ${NEW_KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml
        fi

        if [[ -d ${NEW_KYLIN_HOME}/write_hadoop_conf ]]; then
          cp -rfv ${OLD_KYLIN_HOME}/write_hadoop_conf/hive-site.xml ${NEW_KYLIN_HOME}/write_hadoop_conf/hiveserver2-site.xml
          cp -rfv ${OLD_KYLIN_HOME}/write_hadoop_conf/hive-site.xml ${NEW_KYLIN_HOME}/write_hadoop_conf/hivemetastore-site.xml
        fi
    fi
    info "...................................................[DONE]"

    # copy spark-env for spark3
    logging "Copy spark-env for spark3"
    if [[ -f ${OLD_KYLIN_HOME}/spark/conf/spark-env.sh ]]; then
        if prompt "'${OLD_KYLIN_HOME}/spark/conf/spark-env.sh' -> '${NEW_KYLIN_HOME}/spark/conf/'"; then
            \cp -vf ${OLD_KYLIN_HOME}/spark/conf/spark-env.sh ${NEW_KYLIN_HOME}/spark/conf/ >> $upgrade_log || fail
        fi
    fi
    info "...................................................[DONE]"

    # sed -nE 's/^([#\t ]*)(kylin\..*|kap\..*)/\2/p' kylin.properties | awk '{kv[substr($0,0,index($0,"=")-1)]=substr($0,index($0,"=")+1)} END{print kv["kylin.metadata.url"]}'
    logging "Checking Kylin Conf"
python <<PY
from __future__ import print_function
import os
import sys
try:
    import commands as cmd
except ImportError:
    import subprocess as cmd

def printer(msg, *outs):
    for o in outs: print(msg, file=o)

def getProp(prop_file):
    if not os.path.exists(prop_file):
        return dict()

    output = cmd.getoutput("sed -nE 's/^([#\\\\t ]*)(kylin\\..*=.*|kap\\..*=.*)/\\\\2/p' %s" % prop_file)
    prop = dict()
    for x in output.split('\n'):
        if x.strip() == '':
            continue
        prop[x[0: x.index('=')]] = x[x.index('=') + 1:]
    return prop

with open('${upgrade_log}', 'a+') as upgrade_log:
    origin_prop = getProp('${NEW_KYLIN_HOME}/conf/kylin.properties')
    prod_prop = dict(getProp('${NEW_KYLIN_HOME}/conf/kylin.properties'), **getProp('${NEW_KYLIN_HOME}/conf/kylin.properties.override'))
    diffs = set(prod_prop.items()) - set(origin_prop.items())

    def logging(msg):
        printer(msg, sys.stdout, upgrade_log)

    for diff in diffs:
        logging(diff)
PY
    info "...................................................[DONE]"

    logging "Install"
    if prompt "'${NEW_KYLIN_HOME}' -> '${OLD_KYLIN_HOME}'"; then
        install_dir=$(dirname $OLD_KYLIN_HOME)
        home_name=$(basename $OLD_KYLIN_HOME)

        # backup
        now=`date '+%Y%m%d%H%M'`
        backup_file=${home_name}_${now}.tar.gz
        cd $install_dir && tar -zcvf ${backup_file} ${home_name} >> $upgrade_log || fail

        # install
        rm -rfv ${OLD_KYLIN_HOME} >> $upgrade_log || fail
        mv -vf ${NEW_KYLIN_HOME} ${OLD_KYLIN_HOME} >> $upgrade_log || fail
        info "...................................................[DONE]"
        recordKylinUpgradeResult "${START_TIME}" "true" "${OLD_KYLIN_HOME}"
        info "Upgrade finished!"
        # needed by km
        info "Backup location:${install_dir}/${backup_file}"
    else
        warn "...................................................[SKIP]"
        recordKylinUpgradeResult "${START_TIME}" "true" "${NEW_KYLIN_HOME}"
        info "Upgrade aborted because you chose to stop"
    fi

}

function recordKylinUpgradeResult() {
    logLevel=`[ "$2" == "true" ] && echo INFO || echo ERROR`
    echo `date '+%Y-%m-%d %H:%M:%S '`"${logLevel} : [Operation: upgrade result] user:`whoami`, upgrade time:$1, success:$2" >> $3/logs/security.log
}

NEW_KYLIN_HOME=$(cd `dirname -- $0` && cd ../ && pwd -P)
silent=1
while [[ $# != 0 ]]; do
    if [[ $1 == "--silent" ]]; then
        silent=0
    else
        OLD_KYLIN_HOME=$(cd $1 && pwd)
    fi
    shift
done

if [[ -z $OLD_KYLIN_HOME ]] || [[ ! -d $OLD_KYLIN_HOME ]]; then
    help
fi

if [[ $OLD_KYLIN_HOME == $NEW_KYLIN_HOME ]]; then
    error "Please specify the old version of the Kyligence Enterprise installation directory."
    help
fi

mkdir -p ${NEW_KYLIN_HOME}/logs
upgrade_log=${NEW_KYLIN_HOME}/logs/upgrade-$(date '+%Y_%m_%d_%H_%M_%S').log

set -o errexit
set -o pipefail
START_TIME=$(date "+%Y-%m-%d %H:%M:%S")
upgrade

