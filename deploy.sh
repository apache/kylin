#!/bin/bash

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
#export hbase configs, most of the configurations are useless now, but KYLIN_HBASE_CONF_PATH is used by SampleCubeSetupAsTest now
hbase org.apache.hadoop.util.RunJar $JOB_JAR_NAME com.kylinolap.job.deployment.HbaseConfigPrinter /tmp/kylin_retrieve.sh
#load config variables
source /tmp/kylin_retrieve.sh

cd $KYLIN_HOME
mkdir -p /etc/kylin

HOSTNAME=`hostname`
DEFAULT_CHECK_URL="kylin.job.yarn.app.rest.check.status.url=http://sandbox"
DEFAULT_SERVER_LIST="kylin.rest.servers=sandbox"
NEW_CHECK_URL_PREFIX="kylin.job.yarn.app.rest.check.status.url=http://"
NEW_SERVER_LIST_PREFIX="kylin.rest.servers="

#escape special characters for sed
DEFAULT_CHECK_URL=$(escape_sed_pattern $DEFAULT_CHECK_URL)
DEFAULT_SERVER_LIST=$(escape_sed_pattern $DEFAULT_SERVER_LIST)
NEW_CHECK_URL_PREFIX=$(escape_sed_replacement $NEW_CHECK_URL_PREFIX)
NEW_SERVER_LIST_PREFIX=$(escape_sed_replacement $NEW_SERVER_LIST_PREFIX)
HOSTNAME=$(escape_sed_replacement $HOSTNAME)

#deploy kylin.properties to /etc/kylin
cat examples/test_case_data/sandbox/kylin.properties | \
    sed -e "s/${DEFAULT_CHECK_URL}/${NEW_CHECK_URL_PREFIX}${HOSTNAME}/g"  | \
    sed -e "s/${DEFAULT_SERVER_LIST}/${NEW_SERVER_LIST_PREFIX}${HOSTNAME}/g"   >  /etc/kylin/kylin.properties

cat examples/test_case_data/sandbox/kylin_job_conf.xml > /etc/kylin/kylin_job_conf.xml

# 1. generate synthetic fact table(test_kylin_fact) data and dump it into hive
# 2. create empty cubes on these data, ready to be built
cd $KYLIN_HOME
mvn test -Dtest=com.kylinolap.job.SampleCubeSetupAsTest -DfailIfNoTests=false

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

echo "Kylin is deployed successfully!!!"
echo ""
echo ""
echo "Please check the configuration:"
echo ""
echo "==================================================================="
cat /etc/kylin/kylin.properties
echo "==================================================================="
echo ""
echo "You can directly modify it by editing /etc/kylin/kylin.properties"
echo "If you're using standard hadoop sandbox, you might keep it untouched"
echo "After checking, please start kylin by using \"./kylin.sh start\""

