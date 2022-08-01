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

KAP_KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.enabled`

function is_kap_kerberos_enabled(){
  if [[ "${KAP_KERBEROS_ENABLED}" == "true" ]];then
    echo 1
    return 1
  else
    echo 0
    return 0
  fi
}

function exportKRB5() {
    KAP_KERBEROS_CACHE=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.cache`
    export KRB5CCNAME=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_CACHE}

    KAP_KERBEROS_KRB5=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.krb5-conf`
    if [ ! -n "$KAP_KERBEROS_KRB5" ]; then
        quit "kylin.kerberos.krb5-conf cannot be set to empty in kylin.properties"
    fi

    export KRB5_CONFIG=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_KRB5}
    echo "KRB5_CONFIG is set to ${KRB5_CONFIG}"
}

function prepareKerberosOpts() {
    export KYLIN_KERBEROS_OPTS=""
    if [[ $(is_kap_kerberos_enabled) == 1 ]];then
      KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
    fi
}

function initKerberos() {
    KAP_KERBEROS_PRINCIPAL=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.principal`
    KAP_KERBEROS_KEYTAB=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.keytab`
    KAP_KERBEROS_KEYTAB_PATH=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_KEYTAB}

    if [ ! -e ${KRB5_CONFIG} ]; then
        quit "${KRB5_CONFIG} file doesn't exist"
    fi

    echo "Kerberos is enabled, init..."
    kinit -kt $KAP_KERBEROS_KEYTAB_PATH $KAP_KERBEROS_PRINCIPAL
}

function prepareJaasConf() {
    if [ -f ${KYLIN_HOME}/conf/jaas.conf ]; then
        return
    fi

    cat > ${KYLIN_HOME}/conf/jaas.conf <<EOL
Client{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=false
    useTicketCache=true
    debug=false;
};

EOL
}

function prepareZKPrincipal() {
    params=`env | grep "HADOOP_OPTS"`
    splitParams=(${params//'-D'/ })
    for param in ${splitParams[@]}
    do
        if [[ "$param" == zookeeper* ]];then
            infos=(${param//'zookeeper.server.principal='/ })
            envZKPrincipal=${infos[0]}
            zkPrincipal=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.zookeeper-server-principal`
            if [ $zkPrincipal != $envZKPrincipal ]
            then
                sed -i '/kap.kerberos.zookeeper.server.principal/d' ${KYLIN_CONFIG_FILE}
                sed -i '/kylin.kerberos.zookeeper-server-principal/d' ${KYLIN_CONFIG_FILE}
                sed -i '$a\kylin.kerberos.zookeeper-server-principal='$envZKPrincipal'' ${KYLIN_CONFIG_FILE}
            fi
        fi
    done
}

function prepareFIKerberosInfoIfNeeded() {
    prepareJaasConf
    KERBEROS_PALTFORM=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.platform`
    if [[ "${KERBEROS_PALTFORM}" == "FI" || "${KERBEROS_PALTFORM}" == "TDH" ]]
    then
        prepareZKPrincipal
    fi
}

function initKerberosIfNeeded(){
    if [[ -n $SKIP_KERB ]]; then
        return
    fi

    export SKIP_KERB=1

    if [[ $(is_kap_kerberos_enabled) == 1 ]]
    then
        if [[ -z "$(command -v klist)" ]]
        then
             quit "Kerberos command not found! Please check configuration of Kerberos in kylin.properties or check Kerberos installation."
        fi

        exportKRB5

        if ! klist -s
        then
            initKerberos
        else
           echo "Kerberos ticket is valid, skip init."
        fi

        prepareFIKerberosInfoIfNeeded

        # check if kerberos init success
        if ! klist -s
        then
            quit "Kerberos ticket is not valid, please run 'kinit -kt <keytab_path> <principal>' manually or set configuration of Kerberos in kylin.properties."
        fi
    fi
}