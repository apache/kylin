#!/bin/bash

# ============================================================================

base=master

# ============================================================================

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

# ============================================================================

git fetch apache
git checkout apache/$base-hbase1.x
git format-patch -1
git checkout apache/$base-cdh1.x
git format-patch -1

git checkout apache/$base
git checkout -b tmp
git reset apache/$base --hard

git am -3 --ignore-whitespace 0001-KYLIN-1528-Create-a-branch-for-v1.5-with-HBase-1.x-A.patch
#git push apache tmp:$base-hbase1.x -f
rm 0001-KYLIN-1528-Create-a-branch-for-v1.5-with-HBase-1.x-A.patch

git am -3 --ignore-whitespace 0001-KYLIN-1672-support-kylin-on-cdh-5.7.patch
#git push apache tmp:$base-cdh5.7 -f
rm 0001-KYLIN-1672-support-kylin-on-cdh-5.7.patch

# clean up
git checkout master
git reset apache/master --hard
git checkout -b tmp
