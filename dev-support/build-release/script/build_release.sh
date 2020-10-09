#!/usr/bin/env bash

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

PACKAGE_ENABLE=false
RELEASE_ENABLE=false

function exit_with_usage {
  cat << EOF
usage: release-build.sh <package|publish-rc>
Creates build deliverables from a Kylin commit.

Top level targets are
  package: Create binary packages and commit them to dist.apache.org/repos/dist/dev/spark/
  publish-rc: Publish snapshot release to Apache snapshots

All other inputs are environment variables

GIT_REF - Release tag or commit to build from
SPARK_PACKAGE_VERSION - Release identifier in top level package directory (e.g. 2.1.2-rc1)
SPARK_VERSION - (optional) Version of Spark being built (e.g. 2.1.2)

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account

GPG_KEY - GPG key used to sign release artifacts
GPG_PASSPHRASE - Passphrase for GPG key
EOF
  exit 1
}

if [ $# -eq 0 ]; then
  exit_with_usage
fi

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

if [[ "$1" == "package" ]]; then
    PACKAGE_ENABLE=true
fi

if [[ "$1" == "publish-rc" ]]; then
    PACKAGE_ENABLE=true
    RELEASE_ENABLE=true
fi

# if PACKAGE_ENABLE and RELEASE_ENABLE == false, exit


set -e
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

####################################################
####################################################
####################################################
####################################################
#### Configuration

export KYLIN_PACKAGE_BRANCH=master
export KYLIN_PACKAGE_BRANCH_HADOOP3=master-hadoop3
export KYLIN_PACKAGE_VERSION=3.1.1
export KYLIN_PACKAGE_VERSION_RC=3.1.1-rc1
export NEXT_RELEASE_VERSION=3.1.2-SNAPSHOT
export ASF_USERNAME=xxyu
export ASF_PASSWORD=123
export GPG_KEY=123
export GPG_PASSPHRASE=123

export source_release_folder=/root/kylin-release-folder/kylin
export binary_release_folder=/root/kylin-release-folder/kylin_bin
export svn_release_folder=/root/dist/dev/kylin

RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/kylin"


####################################################
####################################################
####################################################
####################################################
#### Prepare source code

cd $source_release_folder
git checkout ${KYLIN_PACKAGE_BRANCH}
git pull --rebase


####################################################
####################################################
####################################################
####################################################
#### Prepare tag & source tarball & upload maven artifact

# Use release-plugin to check license & build source package & build and upload maven artifact
mvn -DskipTests -DreleaseVersion=${KYLIN_PACKAGE_VERSION} -DdevelopmentVersion=${NEXT_RELEASE_VERSION}-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" release:prepare
mvn -DskipTests -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" release:perform


####################################################
####################################################
####################################################
####################################################
####

# Create a directory for this release candidate
mkdir ${svn_release_folder}/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}
rm -rf target/apache-kylin-*ource-release.zip.asc.sha256

# Move source code and signture of source code to release candidate directory
cp target/apache-kylin-*source-release.zip* ${svn_release_folder}/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}

# Go to package directory
cd $binary_release_folder
git checkout ${KYLIN_PACKAGE_BRANCH}
git pull --rebase

# Checkout to tag, which is created by maven-release-plugin
# Commit message looks like "[maven-release-plugin] prepare release kylin-4.0.0-alpha"
git checkout kylin-${RELEASE_VERSION}


####################################################
####################################################
####################################################
####################################################
#### Build binary


# Build first packages
build/script/package.sh
tar -zxf dist/apache-kylin-${RELEASE_VERSION}-bin.tar.gz
mv apache-kylin-${RELEASE_VERSION}-bin apache-kylin-${RELEASE_VERSION}-bin-hadoop2
tar -cvzf ~/dist/dev/kylin/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}/apache-kylin-${RELEASE_VERSION}-bin-hadoop2.tar.gz apache-kylin-${RELEASE_VERSION}-bin-hadoop2
rm -rf apache-kylin-${RELEASE_VERSION}-bin-hadoop2

build/script/package.sh -P cdh5.7
tar -zxf dist/apache-kylin-${RELEASE_VERSION}-bin.tar.gz
mv apache-kylin-${RELEASE_VERSION}-bin apache-kylin-${RELEASE_VERSION}-bin-cdh57
tar -cvzf ~/dist/dev/kylin/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}/apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz apache-kylin-${RELEASE_VERSION}-bin-cdh57
rm -rf apache-kylin-${RELEASE_VERSION}-bin-cdh57


####################################################
####################################################
####################################################
####################################################
#### Sign binary

cd ~/dist/dev/kylin/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}
gpg --armor --output apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz.asc --detach-sig apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz
shasum -a 256 apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz > apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz.sha256

gpg --armor --output apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz.asc --detach-sig apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz
shasum -a 256 apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz > apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz.sha256

####################################################
####################################################
####################################################
####################################################
#### Upload to svn repository

cd ..
svn add apache-kylin-${KYLIN_PACKAGE_VERSION_RC}
svn commit -m 'Checkin release artifacts for '${KYLIN_PACKAGE_VERSION_RC}