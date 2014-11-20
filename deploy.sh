#!/bin/sh

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions

function error() {
SCRIPT="$0"           # script name
LASTLINE="$1"         # line of error occurrence
LASTERR="$2"          # error code
echo "ERROR exit from ${SCRIPT} : line ${LASTLINE} with exit code ${LASTERR}"
exit 1
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
    echo "Please set $JAVA_HOME so that Kylin-Deploy can proceed"
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
#generate config variables
hbase org.apache.hadoop.util.RunJar $JOB_JAR_NAME com.kylinolap.job.deployment.HbaseConfigPrinter /tmp/kylin_retrieve.sh
#load config variables
source /tmp/kylin_retrieve.sh

cd $KYLIN_HOME
mkdir -p /etc/kylin

HOSTNAME=`hostname`
CLI_HOSTNAME_DEFAULT="kylin.job.remote.cli.hostname=sandbox.hortonworks.com"
CLI_PASSWORD_DEFAULT="kylin.job.remote.cli.password=hadoop"
METADATA_URL_DEFAULT="kylin.metadata.url=kylin_metadata_qa@hbase:sandbox.hortonworks.com:2181:/hbase-unsecure"
STORAGE_URL_DEFAULT="kylin.storage.url=hbase:sandbox.hortonworks.com:2181:/hbase-unsecure"
CHECK_URL_DEFAULT="kylin.job.yarn.app.rest.check.status.url=http://sandbox"


NEW_CLI_HOSTNAME_PREFIX="kylin.job.remote.cli.hostname="
NEW_CLI_PASSWORD_PREFIX="kylin.job.remote.cli.password="
NEW_METADATA_URL_PREFIX="kylin.metadata.url=kylin_metadata_qa@hbase:"
NEW_STORAGE_URL_PREFIX="kylin.storage.url=hbase:"
NEW_CHECK_URL_PREFIX="kylin.job.yarn.app.rest.check.status.url=http://"

KYLIN_ZOOKEEPER_URL=${KYLIN_ZOOKEEPER_QUORUM}:${KYLIN_ZOOKEEPER_CLIENT_PORT}:${KYLIN_ZOOKEEPER_ZNODE_PARENT}

echo "Kylin install script requires root password for ${HOSTNAME}"
echo "(The default root password for hortonworks VM is hadoop, and for cloudera VM is cloudera)"

[[ "$SILENT" ]] || read -s -p "Enter Password for root: " ROOTPASS

#deploy kylin.properties to /etc/kylin
cat examples/test_case_data/kylin.properties | \
    sed -e "s,${CHECK_URL_DEFAULT},${NEW_CHECK_URL_PREFIX}${HOSTNAME}," | \
    sed -e "s,${CLI_HOSTNAME_DEFAULT},${NEW_CLI_HOSTNAME_PREFIX}${HOSTNAME}," | \
    sed -e "s,${CLI_PASSWORD_DEFAULT},${NEW_CLI_PASSWORD_PREFIX}${ROOTPASS}," | \
    sed -e "s,${METADATA_URL_DEFAULT},${NEW_METADATA_URL_PREFIX}${KYLIN_ZOOKEEPER_URL}," | \
    sed -e "s,${STORAGE_URL_DEFAULT},${NEW_STORAGE_URL_PREFIX}${KYLIN_ZOOKEEPER_URL}," >  /etc/kylin/kylin.properties


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

sudo -i "${CATALINA_HOME}/bin/shutdown.sh" || true # avoid trapping
cd $KYLIN_HOME/server/target
WAR_NAME="kylin.war"
rm -rf $CATALINA_HOME/webapps/kylin
rm -f $CATALINA_HOME/webapps/$WAR_NAME
cp $KYLIN_HOME/server/target/$WAR_NAME $CATALINA_HOME/webapps/
cd $CATALINA_HOME/webapps;
chmod 644 $WAR_NAME;
echo "REST service deployed"

rm -rf /var/www/html/kylin
mkdir -p /var/www/html/kylin
cd $KYLIN_HOME/
tar -xf webapp/dist/Web.tar -C /var/www/html/kylin
echo "Web deployed"

cd $KYLIN_HOME/
#deploy setenv.sh
rm -rf $CATALINA_HOME/bin/setenv.sh
echo JAVA_OPTS=\"-Djava.library.path=${KYLIN_LD_LIBRARY_PATH}\" >> ${CATALINA_HOME}/bin/setenv.sh
echo CATALINA_OPTS=\"-Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true -Dspring.profiles.active=sandbox \" >> ${CATALINA_HOME}/bin/setenv.sh
echo CLASSPATH=\"${CATALINA_HOME}/lib/*:${KYLIN_HBASE_CLASSPATH}:/etc/kylin\" >> ${CATALINA_HOME}/bin/setenv.sh
echo "setenv.sh created"

#deploy server.xml
rm -rf ${CATALINA_HOME}/conf/server.xml
cp deploy/server.xml ${CATALINA_HOME}/conf/server.xml
echo "server.xml copied"

#deploy web.xml
rm -rf ${CATALINA_HOME}/conf/web.xml
cp deploy/web.xml ${CATALINA_HOME}/conf/web.xml
echo "web.xml copied"

echo "Tomcat ready"

# redeploy coprocessor
#hbase org.apache.hadoop.util.RunJar /usr/lib/kylin/kylin-job-latest.jar com.kylinolap.job.tools.DeployCoprocessorCLI /usr/lib/kylin/kylin-coprocessor-latest.jar


sudo -i "${CATALINA_HOME}/bin/startup.sh"


echo "Kylin-Deploy Success!"
echo "Please visit http://<your_sandbox_ip>:7070 to play with the cubes! (Useranme: ADMIN, Password: KYLIN)"
