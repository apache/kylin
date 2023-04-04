#!/bin/bash

#
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#

export LC_ALL=C.UTF-8
export LANG=C.UTF-8
set -e

function run_command {
  local BANNER="$1"
  shift 1

  echo "========================"
  echo "==> $BANNER"
  echo "Command: $@"

  "$@" 2>&1

  local EC=$?
  if [ $EC != 0 ]; then
    echo "Command FAILED : $@, please check!!!"
    exit $EC
  fi
}

####################################################
####################################################
#### Release Configuration

source /root/scripts/setenv.sh
if [ $# -eq 0 ]; then
  RELEASE_STEP="publish-rc"
else
  RELEASE_STEP=$1
fi
echo "==> Running step : $RELEASE_STEP"

GIT_BRANCH=${GIT_BRANCH:-kylin5}
ASF_USERNAME=${ASF_USERNAME:-xxyu}
RELEASE_VERSION=${RELEASE_VERSION:-5.0.0-alpha}
NEXT_RELEASE_VERSION=${NEXT_RELEASE_VERSION:-5.0.0-beta}

export working_dir=/root/kylin-release-folder
export source_code_folder=$working_dir/kylin
export packaging_folder=$source_code_folder/target/checkout
export svn_folder=$working_dir/svn
export rc_name=apache-kylin-"${RELEASE_VERSION}"-${RC_NAME}
export release_candidate_folder=$svn_folder/$rc_name

export ASF_KYLIN_REPO="gitbox.apache.org/repos/asf/kylin.git"
# GITHUB_REPO_URL=${GIT_REPO_URL:-https://github.com/apache/kylin.git}
export RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/kylin"
export RELEASE_LOCATION="https://dist.apache.org/repos/dist/release/kylin"

####################################################
####################################################
#### ASF Confidential

echo "==> Check ASF confidential"

if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

if [[ -z "$GPG_PASSPHRASE" ]]; then
  echo 'The environment variable GPG_PASSPHRASE is not set. Enter the passphrase to'
  echo 'unlock the GPG signing key that will be used to sign the release!'
  echo
  stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
fi

echo "==> Init Git Configuration"
mkdir -p $working_dir
git config --global user.name "${GIT_USERNAME}"
git config --global user.email "${ASF_USERNAME}"@apache.org
git config --global user.password ${ASF_PASSWORD}

####################################################
####################################################
#### Prepare source code

if [[ "$RELEASE_STEP" == "publish-rc" ]]; then
  echo "==> Clone kylin source for $RELEASE_VERSION"

  cd $working_dir

  if [ ! -d "${source_code_folder}" ]
  then
      echo "Clone source code to ${source_code_folder} ."
      run_command "Clone Gitbox" git clone "https://$ASF_USERNAME:$ASF_PASSWORD@$ASF_KYLIN_REPO" -b "$GIT_BRANCH"
  fi

  if [ ! -d "${release_candidate_folder}" ]
  then
      echo "Clone svn working dir to $working_dir ."
      run_command "Clone ASF SVN" svn co $RELEASE_STAGING_LOCATION $svn_folder
  fi
fi

if [[ "$RELEASE_STEP" == "reset" ]]; then
  echo "==> reset release folder"
  cd ${source_code_folder}
  git reset --hard HEAD~5
  git pull -r origin "$GIT_BRANCH"
  mvn clean
  mvn release:clean
fi

####################################################
####################################################
#### Publish maven artifact and source package

if [[ "$RELEASE_STEP" == "publish-rc" ]]; then
  echo "==> publish-release-candidate source code"
  # Go to source directory
  cd ${source_code_folder}

  tag_exist=`git tag --list | grep kylin-"${RELEASE_VERSION}" | wc -l`
  if [[ $tag_exist != 0 ]]; then
     echo "Delete local tag"
     git tag --delete kylin-"${RELEASE_VERSION}"
  fi

  ## Prepare tag & source tarball & upload maven artifact
  # Use release-plugin to check license & build source package & build and upload maven artifact
  # https://maven.apache.org/maven-release/maven-release-plugin/examples/prepare-release.html
  # https://infra.apache.org/publishing-maven-artifacts.html
  # Use `mvn release:clean`  if you want to prepare again
  run_command "Maven Release Prepare" mvn -DskipTests -DreleaseVersion="${RELEASE_VERSION}" \
    -DdevelopmentVersion="${NEXT_RELEASE_VERSION}"-SNAPSHOT -Papache-release,nexus -DdryRun=${DRY_RUN} \
    -Darguments="-Dmaven.javadoc.skip=true -Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" \
    release:prepare
  run_command "Maven Release Perform" mvn -DskipTests -Papache-release,nexus \
    -Darguments="-Dmaven.javadoc.skip=true -Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" \
    release:perform

  # Create a directory for this release candidate
  mkdir -p ${release_candidate_folder}
  rm -rf target/apache-kylin-*ource-release.zip.asc.sha256

  # Move source code and signture of source code to release candidate directory
  cp target/apache-kylin-*source-release.zip* "${release_candidate_folder}"
fi

####################################################
####################################################
#### Build Binary

if [[ "$RELEASE_STEP" == "publish-rc" ]]; then
  echo "==> Building kylin binary for $RELEASE_VERSION"
  cd ${packaging_folder}

  export release_version=$RELEASE_VERSION
  run_command "Build binary" bash build/release/release.sh -official -noSpark

  cp dist/apache-kylin-*.tar.gz "${release_candidate_folder}"
fi

####################################################
####################################################
#### Publish binary to release candidate folder

if [[ "$RELEASE_STEP" == "publish-rc" ]]; then
  ## Sign binary
  echo "==> publish-release-candidate binary"
  cd "${release_candidate_folder}"
  run_command "Sign binary" gpg --armor --output apache-kylin-"${RELEASE_VERSION}"-bin.tar.gz.asc --detach-sig apache-kylin-${RELEASE_VERSION}-bin.tar.gz
  shasum -a 256 apache-kylin-"${RELEASE_VERSION}"-bin.tar.gz > apache-kylin-${RELEASE_VERSION}-bin.tar.gz.sha256

  ## Upload to svn repository
  cd ${svn_folder}
  svn add ${rc_name}
  run_command "Publish release candidate dir" svn commit -m 'Check in release artifacts for '${rc_name}
fi

####################################################
####################################################
#### Publish binary to release folder after vote passed

if [[ "$RELEASE_STEP" == "publish-release" ]]; then
  echo "==> publish-release"
  # todo
fi