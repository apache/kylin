#!/usr/bin/env bash

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions

function error() {
SCRIPT="$0"           # script name
LASTLINE="$1"         # line of error occurrence
LASTERR="$2"          # error code
echo "ERROR exit from ${SCRIPT} : line ${LASTLINE} with exit code ${LASTERR}"
exit 1
}

function escape_sed_replacement(){
        v=$1
        v=$(sed -e 's/[\/&]/\\&/g' <<< $v)
        echo $v
}

function escape_sed_pattern(){
        v=$1
        v=$(sed -e 's/[]\/$*.^|[]/\\&/g' <<< $v)
        echo $v
}

trap 'error ${LINENO} ${?}' ERR

echo ""
echo "Welcome to use Kylin-Deploy script"
echo "This script will help you:"
echo "1. Check environment"
echo "2. Build Kylin artifacts"
echo "3. Prepare test cube related data"
echo "4. Lauch a web service to build cube and query with (at http://localhost:7070)"
echo "Please make sure you are running this script on a hadoop CLI machine, and you have enough permissions."
echo "Also, We assume you have installed: JAVA, TOMCAT, NPM and MAVEN."
echo "[Warning] The installation may break existing tomcat applications on this CLI"
echo ""


[[ "$SILENT" ]] || ( read -p "Are you sure you want to proceed?(press Y or y to confirm) " -n 1 -r
echo    # (optional) move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Not going to proceed, quit without finishing! You can rerun the script to have another try."
    exit 1
fi )

echo "Checking JAVA status..."

if [ -z "$JAVA_HOME" ]
then
    echo "Please set JAVA_HOME so that Kylin-Deploy can proceed"
    exit 1
else
    echo "JAVA_HOME is set to $JAVA_HOME"
fi

if [ -d "$JAVA_HOME" ]
then
    echo "$JAVA_HOME exists"
else
    echo " $JAVA_HOME does not exist or is not a directory."
    exit 1
fi

echo "Checking tomcat status..."

if [ -z "$CATALINA_HOME" ]
then
    echo "Please set CATALINA_HOME so that Kylin-Deploy knows where to start tomcat"
    exit 1
else
    echo "CATALINA_HOME is set to $CATALINA_HOME"
fi

if [ -d "$CATALINA_HOME" ]
then
    echo "$CATALINA_HOME exists"
else
    echo " $CATALINA_HOME does not exist or is not a directory."
    exit 1
fi

echo "Checking maven..."

if [ -z "$(command -v mvn)" ]
then
    echo "Please install maven first so that Kylin-Deploy can proceed"
    exit 1
else
    echo "maven check passed"
fi

echo "Checking npm..."

if [ -z "$(command -v npm)" ]
then
    echo "Please install npm first so that Kylin-Deploy can proceed"
    exit 1
else
    echo "npm check passed"
fi

KYLIN_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
echo "Kylin home folder path is $KYLIN_HOME"
cd $KYLIN_HOME


echo "Building and packaging..."
source ./package.sh

echo "retrieving classpath..."
cd $KYLIN_HOME/job/target
JOB_JAR_NAME="kylin-job-latest.jar"
#export hbase configs, most of the configurations are useless now, but KYLIN_HBASE_CONF_PATH is used by SampleCubeSetupTest now
hbase org.apache.hadoop.util.RunJar $JOB_JAR_NAME com.kylinolap.job.deployment.HbaseConfigPrinter /tmp/kylin_retrieve.sh
#load config variables
source /tmp/kylin_retrieve.sh

cd $KYLIN_HOME
mkdir -p /etc/kylin

HOSTNAME=`hostname`
CLI_HOSTNAME_DEFAULT="kylin.job.remote.cli.hostname=sandbox.hortonworks.com"
CLI_PASSWORD_DEFAULT="kylin.job.remote.cli.password=hadoop"
CHECK_URL_DEFAULT="kylin.job.yarn.app.rest.check.status.url=http://sandbox"


NEW_CLI_HOSTNAME_PREFIX="kylin.job.remote.cli.hostname="
NEW_CLI_PASSWORD_PREFIX="kylin.job.remote.cli.password="
NEW_CHECK_URL_PREFIX="kylin.job.yarn.app.rest.check.status.url=http://"

echo "Kylin install script requires root password for ${HOSTNAME}"
echo "(The default root password for hortonworks VM is hadoop, and for cloudera VM is cloudera)"

[[ "$SILENT" ]] || read -r -s -p  "Enter Password for root: " ROOTPASS

#escape special characters for sed
CHECK_URL_DEFAULT=$(escape_sed_pattern $CHECK_URL_DEFAULT)
CLI_HOSTNAME_DEFAULT=$(escape_sed_pattern $CLI_HOSTNAME_DEFAULT)
CLI_PASSWORD_DEFAULT=$(escape_sed_pattern $CLI_PASSWORD_DEFAULT)

NEW_CHECK_URL_PREFIX=$(escape_sed_replacement $NEW_CHECK_URL_PREFIX)
NEW_CLI_HOSTNAME_PREFIX=$(escape_sed_replacement $NEW_CLI_HOSTNAME_PREFIX)
NEW_CLI_PASSWORD_PREFIX=$(escape_sed_replacement $NEW_CLI_PASSWORD_PREFIX)
HOSTNAME=$(escape_sed_replacement $HOSTNAME)
ROOTPASS=$(escape_sed_replacement $ROOTPASS)



#deploy kylin.properties to /etc/kylin
cat examples/test_case_data/sandbox/kylin.properties | \
    sed -e "s/${CHECK_URL_DEFAULT}/${NEW_CHECK_URL_PREFIX}${HOSTNAME}/g" | \
    sed -e "s/${CLI_HOSTNAME_DEFAULT}/${NEW_CLI_HOSTNAME_PREFIX}${HOSTNAME}/g" | \
    sed -e "s/${CLI_PASSWORD_DEFAULT}/${NEW_CLI_PASSWORD_PREFIX}${ROOTPASS}/g" >  /etc/kylin/kylin.properties


echo "a copy of kylin config is generated at /etc/kylin/kylin.properties:"
echo "==================================================================="
cat /etc/kylin/kylin.properties
echo ""
echo "==================================================================="
echo ""

[[ "$SILENT" ]] || ( read -p "please ensure the CLI address/username/password is correct, and press y to proceed: " -n 1 -r
echo    # (optional) move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Not going to proceed, quit without finishing! You can rerun the script to have another try."
    exit 1
fi )

# 1. generate synthetic fact table(test_kylin_fact) data and dump it into hive
# 2. create empty cubes on these data, ready to be built
cd $KYLIN_HOME
mvn test -Dtest=com.kylinolap.job.SampleCubeSetupTest -DfailIfNoTests=false

#overwrite server.xml
mv ${CATALINA_HOME}/conf/server.xml ${CATALINA_HOME}/conf/server.xml.bak
cp deploy/server.xml ${CATALINA_HOME}/conf/server.xml
echo "server.xml overwritten..."

#deploy kylin.war
rm -rf $CATALINA_HOME/webapps/kylin
rm -f $CATALINA_HOME/webapps/kylin.war
cp $KYLIN_HOME/server/target/kylin.war $CATALINA_HOME/webapps/
chmod 644 $CATALINA_HOME/webapps/kylin.war
echo "Tomcat war deployed..."

#start tomcat service from hbase runjar
export HBASE_CLASSPATH_PREFIX=/etc/kylin:${CATALINA_HOME}/bin/bootstrap.jar:${CATALINA_HOME}/bin/tomcat-juli.jar:${CATALINA_HOME}/lib/*:$HBASE_CLASSPATH_PREFIX
hbase -Djava.util.logging.config.file=${CATALINA_HOME}/conf/logging.properties -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
    -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
    -Dspring.profiles.active=sandbox -Djava.endorsed.dirs=${CATALINA_HOME}/endorsed  -Dcatalina.base=${CATALINA_HOME} \
    -Dcatalina.home=${CATALINA_HOME} -Djava.io.tmpdir=${CATALINA_HOME}/temp  \
    -Djava.library.path=${KYLIN_LD_LIBRARY_PATH} \
    org.apache.hadoop.util.RunJar ${CATALINA_HOME}/bin/bootstrap.jar  org.apache.catalina.startup.Bootstrap start > ${CATALINA_HOME}/logs/kylin_sandbox.log 2>&1 &

echo "Kylin is launched successfully!!!"
echo "Please visit http://<your_sandbox_ip>:7070 to play with the cubes! (Useranme: ADMIN, Password: KYLIN)"
