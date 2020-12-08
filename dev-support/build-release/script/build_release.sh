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

export LC_ALL=C.UTF-8
export LANG=C.UTF-8
set -e
PACKAGE_ENABLE=1
RELEASE_ENABLE=0

function exit_with_usage {
  cat << EOF
usage: build_release.sh <package|publish-rc>
Creates build deliverables from a Kylin commit.

Top level targets are
  package: Create binary packages and commit them to dist.apache.org/repos/dist/dev/kylin/
  publish-rc: Publish snapshot release to Apache snapshots

All other inputs are environment variables

GIT_REF - Release tag or commit to build from
RELEASE_VERSION - Release identifier in top level package directory (e.g. 3.1.2)

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
    PACKAGE_ENABLE=1
fi

if [[ "$1" == "publish-rc" ]]; then
    PACKAGE_ENABLE=1
    RELEASE_ENABLE=1
fi

####################################################
####################################################
#### Configuration

KYLIN_PACKAGE_BRANCH=${GIT_BRANCH:-master}
KYLIN_PACKAGE_BRANCH_HADOOP3=${GIT_BRANCH_HADOOP3:-master-hadoop3}
ASF_USERNAME=${ASF_USERNAME:-xxyu}
RELEASE_VERSION=${RELEASE_VERSION:-3.1.2}
NEXT_RELEASE_VERSION=${NEXT_RELEASE_VERSION:-3.1.3}
RUNNING_CI=${RUNNING_CI:-1}
GIT_REPO_URL=${GIT_REPO_URL:-https://github.com/apache/kylin.git}

export source_release_folder=/root/kylin-release-folder/
export binary_release_folder=/root/kylin-release-folder/kylin_bin
export svn_release_folder=/root/dist/dev/kylin
export ci_package_folder=/root/ci

RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/kylin"

####################################################
####################################################
#### Prepare source code

mkdir -p $source_release_folder
mkdir -p $binary_release_folder
mkdir -p $svn_release_folder
mkdir -p $ci_package_folder

cd $source_release_folder
git clone $GIT_REPO_URL
cd kylin
git checkout ${KYLIN_PACKAGE_BRANCH}
git pull --rebase

cp /root/apache-tomcat-7.0.100.tar.gz $source_release_folder/kylin/build


if [[ "$RELEASE_ENABLE" == "1" ]]; then
  echo "Build with maven-release-plugin ..."
  ## Prepare tag & source tarball & upload maven artifact

  # Use release-plugin to check license & build source package & build and upload maven artifact
  #mvn -DskipTests -DreleaseVersion=${KYLIN_PACKAGE_VERSION} -DdevelopmentVersion=${NEXT_RELEASE_VERSION}-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" release:prepare
  #mvn -DskipTests -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" release:perform

  # Create a directory for this release candidate
  #mkdir ${svn_release_folder}/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}
  #rm -rf target/apache-kylin-*ource-release.zip.asc.sha256

  # Move source code and signture of source code to release candidate directory
  #cp target/apache-kylin-*source-release.zip* ${svn_release_folder}/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}

  # Go to package directory
  #cd $binary_release_folder
  #git checkout ${KYLIN_PACKAGE_BRANCH}
  #git pull --rebase

  # Checkout to tag, which is created by maven-release-plugin
  # Commit message looks like "[maven-release-plugin] prepare release kylin-4.0.0-alpha"
  #git checkout kylin-${RELEASE_VERSION}
fi

####################################################
####################################################
#### Build binary

# Build first package for Hadoop2
build/script/package.sh
if [[ "$RUNNING_CI" == "1" ]]; then
    cp dist/apache-kylin-${RELEASE_VERSION}-bin.tar.gz ${ci_package_folder}
    cd ${ci_package_folder}
    tar -zxf apache-kylin-${RELEASE_VERSION}-bin.tar.gz
    mv apache-kylin-${RELEASE_VERSION}-bin apache-kylin-bin
    tar -cvzf apache-kylin-bin.tar.gz apache-kylin-bin
    rm -rf apache-kylin-${RELEASE_VERSION}-bin
    cd -
fi

if [[ "$RELEASE_ENABLE" == "1" ]]; then
  tar -zxf dist/apache-kylin-${RELEASE_VERSION}-bin.tar.gz
  mv apache-kylin-${RELEASE_VERSION}-bin apache-kylin-${RELEASE_VERSION}-bin-hbase1x
  tar -cvzf ~/dist/dev/kylin/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}/apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz apache-kylin-${RELEASE_VERSION}-bin-hbase1x
  rm -rf apache-kylin-${RELEASE_VERSION}-bin-hbase1x
fi

#build/script/package.sh -P cdh5.7
#tar -zxf dist/apache-kylin-${RELEASE_VERSION}-bin.tar.gz
#mv apache-kylin-${RELEASE_VERSION}-bin apache-kylin-${RELEASE_VERSION}-bin-cdh57
#tar -cvzf ~/dist/dev/kylin/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}/apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz apache-kylin-${RELEASE_VERSION}-bin-cdh57
#rm -rf apache-kylin-${RELEASE_VERSION}-bin-cdh57

####################################################
####################################################
#### Release binary

if [[ "$RELEASE_ENABLE" == "1" ]]; then
  ## Sign binary
  cd ~/dist/dev/kylin/apache-kylin-${KYLIN_PACKAGE_VERSION_RC}
  gpg --armor --output apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz.asc --detach-sig apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz
  shasum -a 256 apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz > apache-kylin-${RELEASE_VERSION}-bin-hbase1x.tar.gz.sha256

  # gpg --armor --output apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz.asc --detach-sig apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz
  # shasum -a 256 apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz > apache-kylin-${RELEASE_VERSION}-bin-cdh57.tar.gz.sha256

  ## Upload to svn repository
  cd ..
  svn add apache-kylin-${KYLIN_PACKAGE_VERSION_RC}
  svn commit -m 'Checkin release artifacts for '${KYLIN_PACKAGE_VERSION_RC}
fi