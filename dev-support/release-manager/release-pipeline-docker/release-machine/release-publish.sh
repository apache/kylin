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

# https://stackoverflow.com/questions/57591432/gpg-signing-failed-inappropriate-ioctl-for-device-on-macos-with-maven
GPG_TTY=$(tty)
export GPG_TTY
CUR_DATE=$(date "+%Y-%m-%d")
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
set -e

function exit_with_usage {
  cat << EOF
usage: release-publish.sh <publish-snapshot|publish-release|preview-site|...>
Creates build deliverables from a Kylin commit.

Top level targets are
  // package:
  publish-snapshot: Publish snapshot release to Apache snapshots
  publish-release: Publish a release to Apache release repo
  reset: Clean when after a fail release attempt
  preview-site: Build Kylin website on docker, so you can check/preview website in localhost:7070
  publish-site: After checked, you can upload content to apache.org
EOF
  exit 0
}

function info {
    cat << EOF
========================
|   $1
EOF
}

function ask_confirm {
  if [ "$MODE" = "batch" ] ;then
    return 0
  fi
  read -p "$1 Will you continue? [y/n] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    info "Exiting."
    exit 1
  fi
}

if [ $# -eq 0 ]; then
  exit_with_usage
else
  if [ "$1" = "reset" ] || \
      [ "$1" = "publish-snapshot" ] || \
      [ "$1" = "publish-release" ] || \
      [ "$1" = "preview-site" ] || \
      [ "$1" = "publish-site" ];
  then
      ask_confirm "You are running step [$1] as release manager"
      RELEASE_STEP=$1
  else
      echo "Your input $1 is not valid."
      exit_with_usage
  fi
fi

function read_config {
  if [ "$MODE" = "batch" ] ;then
    echo "$2"
  elif [ "$MODE" = "" ]; then
    local PROMPT="$1"
    local DEFAULT="$2"
    local REPLY=

    read -p "$PROMPT [default is $DEFAULT]: " REPLY
    local RETVAL="${REPLY:-$DEFAULT}"
    if [ -z "$RETVAL" ]; then
      error "$PROMPT is must be provided."
    fi
    echo "$RETVAL"
  fi
}

function run_command {
  local BANNER="$1"
  shift 1

  echo "========================"
  echo "|   $BANNER"
  echo "|   $(date) Command: $@"

  if [ "$LOG_OPTION" = "log" ] ;then
    echo "Redirect to ${LOG} ..."
    "$@">> ${LOG} 2>&1
  else
    "$@" 2>&1
  fi
  local EC=$?
  if [ $EC != 0 ]; then
    echo "Command FAILED : $@, please check!!!"
    exit $EC
  fi
}

function switch_node_for_packaging {
  # nvm use system
  node -v # 12.22.12
}

function switch_node_for_website {
  # nvm install 16.14.2
  # nvm use 16.14.2
  node -v # 16.14.2
}

DRY_RUN=false

ASF_USERNAME=$(read_config "Your apache id?" "$ASF_USERNAME")
GIT_USERNAME=$(read_config "Your full name(used as author of git commit)?" "$GIT_USERNAME")
ASF_PASSWORD=$(read_config "Your apache password?" "$ASF_PASSWORD")
GIT_EMAIL=$ASF_USERNAME"@apache.org"
GPG_KEY=$(read_config "GPG key of you(used to sign release candidate)?" "$GPG_KEY")
GPG_PASSPHRASE=$(read_config "PASSPHRASE for your private GPG key?" "$GPG_PASSPHRASE")

GIT_BRANCH=$(read_config "Git branch for release?" "$GIT_BRANCH")
RELEASE_VERSION=$(read_config "Which version are you going to release?" "$RELEASE_VERSION")
NEXT_RELEASE_VERSION=$(read_config "Which version is the next development version?" "$NEXT_RELEASE_VERSION")
RC_NUMBER="rc"$(read_config "Number for release candidate?" "$RC_NUMBER")

export working_dir=/root/kylin-folder
svn_folder=$working_dir/svn
source_code_folder=$working_dir/source/kylin

packaging_folder=$source_code_folder/target/checkout
svn_stage_folder=$svn_folder/dev
rc_name=apache-kylin-"${RELEASE_VERSION}"-${RC_NUMBER}
release_candidate_folder=$svn_stage_folder/$rc_name
final_release_folder=$svn_folder/release

branch_doc_1=document
branch_doc_2=doc5.0
document_folder=$working_dir/document
document_folder_src=$document_folder/src
document_folder_elder=$document_folder_src/$branch_doc_1
document_folder_newer=$document_folder_src/$branch_doc_2
document_folder_svn=$svn_folder/site

LOG=$working_dir/$rc_name-$CUR_DATE.log
rm -rf "$LOG"

ASF_KYLIN_REPO="gitbox.apache.org/repos/asf/kylin.git"
# GITHUB_REPO_URL=${GIT_REPO_URL:-https://github.com/apache/kylin.git}
RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/kylin"
RELEASE_LOCATION="https://dist.apache.org/repos/dist/release/kylin"
WEBSITE_SVN="https://svn.apache.org/repos/asf/kylin/site"

GPG_COMMAND="gpg -u $GPG_KEY --no-tty --batch --pinentry-mode loopback"

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

for env in ASF_USERNAME GPG_PASSPHRASE RC_NUMBER GPG_KEY; do
  if [ -z "${!env}" ]; then
    echo "ERROR: $env must be set to run this script"
    exit_with_usage
  fi
done

function reset_release {
  info "Reset release folder"
  cd ${source_code_folder}
  git reset --hard HEAD~5
  git pull -r origin "$GIT_BRANCH"
  mvn clean
  mvn release:clean
  # Update current script
  mv /root/release-publish.sh .release-publish.sh.bak
  cp $source_code_folder/build/release/release-pipeline-docker/release-machine/release-publish.sh /root/
}

function prepare_release {
  info "Configuration and Clone Code"
  git config --global user.name "${GIT_USERNAME}"
  git config --global user.email ${GIT_EMAIL}
  git config --global user.password ${ASF_PASSWORD}
  mkdir -p $working_dir
  cd $working_dir

  if [ ! -d "${source_code_folder}" ]
  then
      mkdir -p ${source_code_folder}
      info "Clone source code to ${source_code_folder} ."
      run_command "Clone Gitbox" git clone "https://$ASF_USERNAME:$ASF_PASSWORD@$ASF_KYLIN_REPO" --single-branch --branch "$GIT_BRANCH" ${source_code_folder}
  fi

  if [ ! -d "${release_candidate_folder}" ]
  then
      mkdir -p "${release_candidate_folder}"
      info "Clone svn working dir to $svn_folder ."
      run_command "Clone ASF SVN" svn co $RELEASE_STAGING_LOCATION $svn_stage_folder
  fi
  switch_node_for_packaging
}

function publish_snapshot_source {
  info "Publish source code, maven artifact, git tag for release candidate"
  cd ${source_code_folder}

  tag_exist=`git tag --list | grep kylin-"${RELEASE_VERSION}" | wc -l`
  if [[ $tag_exist != 0 ]]; then
     echo "Delete local and remote tag"
     git tag --delete kylin-"${RELEASE_VERSION}"
     git push --delete origin kylin-"${RELEASE_VERSION}"
  fi

  ## Prepare tag & source tarball & upload maven artifact
  # Use release-plugin to check license & build source package & build and upload maven artifact
  # https://maven.apache.org/maven-release/maven-release-plugin/examples/prepare-release.html
  # https://infra.apache.org/publishing-maven-artifacts.html
  # Use `mvn release:clean`  if you want to prepare again
  maven_options="-DskipTests \
      -DreleaseVersion=${RELEASE_VERSION} \
      -DdevelopmentVersion=${NEXT_RELEASE_VERSION}-SNAPSHOT \
      -Papache-release,nexus \
      -DdryRun=${DRY_RUN} \
      -Dmaven.javadoc.skip=true \
      -Dgpg.passphrase=${GPG_PASSPHRASE} \
      -DgpgArguments=--no-tty --batch --pinentry-mode loopback \
      -Dkeyname=$GPG_KEY"
  run_command "Maven Release Prepare" mvn "${maven_options}" release:prepare
  run_command "Maven Release Perform" mvn "${maven_options}" release:perform

  # Create a directory for this release candidate
  mkdir -p ${release_candidate_folder}
  rm -rf target/apache-kylin-*ource-release.zip.asc.sha256

  # Move source code and signature of source code to release candidate directory
  cp target/apache-kylin-*source-release.zip* "${release_candidate_folder}"
}

function publish_snapshot_package {
  info "Building kylin binary for $RELEASE_VERSION"
  cd ${packaging_folder}

  export release_version=$RELEASE_VERSION
  run_command "Build binary" bash build/release/release.sh -official -noSpark

  cp dist/apache-kylin-*.tar.gz "${release_candidate_folder}"

  ## Sign binary
  echo "publish-release-candidate binary"
  cd "${release_candidate_folder}"
  run_command "Sign binary" echo $GPG_PASSPHRASE | $GPG_COMMAND --passphrase-fd 0 --armour --output apache-kylin-"${RELEASE_VERSION}"-bin.tar.gz.asc --detach-sig apache-kylin-${RELEASE_VERSION}-bin.tar.gz
  shasum -a 512 apache-kylin-"${RELEASE_VERSION}"-bin.tar.gz > apache-kylin-${RELEASE_VERSION}-bin.tar.gz.sha512


  ## Upload to svn repository
  ask_confirm "You are going to upload release candidate, are you sure you have the right permission?"
  cd ${svn_stage_folder}
  svn add ${rc_name}
  run_command "Publish release candidate dir" svn commit --password ${ASF_PASSWORD} -m 'Check in release artifacts for '${rc_name}
  echo "Please check $RELEASE_STAGING_LOCATION"
  return 0
}

function publish_release {
  info "Publish release candidate after vote succeed."
  svn co $RELEASE_LOCATION $final_release_folder
}


####################################################
####################################################
# Following is for website publish

function preview_site() {
  info "Prepare website"
  if [ ! -d "${document_folder_src}" ]; then
      mkdir -p $document_folder_src
      run_command "Install nodejs for docusaurus" switch_node_for_website
  fi
  cd $document_folder_src
  if [ ! -d "${document_folder_elder}" ]; then
      run_command "Clone website for kylin4" git clone --single-branch --branch $branch_doc_1 "https://$ASF_USERNAME:$ASF_PASSWORD@$ASF_KYLIN_REPO" $branch_doc_1
  else
      cd ${document_folder_elder}
      git reset --hard HEAD~4
      git pull -r origin $branch_doc_1
  fi

  if [ ! -d "${document_folder_newer}" ]; then
      run_command "Clone website for kylin5" git clone --single-branch --branch $branch_doc_2 "https://$ASF_USERNAME:$ASF_PASSWORD@$ASF_KYLIN_REPO" $branch_doc_2
  else
      cd ${document_folder_newer}
      git reset --hard HEAD~4
      git pull -r origin $branch_doc_2
  fi

  if [ ! -d "${document_folder_svn}" ]; then
      mkdir ${document_folder_svn}
      run_command "Checkout website files from svn" svn co $WEBSITE_SVN ${document_folder_svn}
  fi

  info "Build website"

  # Build inner website
  cd ${document_folder_newer}/website
  # nvm use 16.14
  run_command "Install node modules" npm install
  run_command "Build inner website" npm run build
  document_folder_newer_build=${document_folder_newer}/website/build

  # Build outer website
  cd ${document_folder_elder}/website
  run_command "Build outer website" jekyll build >>$LOG 2>&1
  document_folder_elder_build=${document_folder_elder}/_site

  # Merge two websites
  rm -rf ${document_folder_elder_build}/5.0
  mv ${document_folder_newer_build} ${document_folder_elder_build}/5.0
  info "Build website should be done, and stored in ${document_folder_elder_build} ."

  run_command "Preview merged website" jekyll s -P 7070
  info "Website could be previewed at localhost:7070"
}

function publish_site() {
  info "Publish website"
#  svn update ${document_folder_svn}
#  svn add --force ${document_folder_svn}/* --auto-props --parents --depth infinity -q
#  svn status ${document_folder_svn}
#  if [ `svn status ${document_folder_svn} | wc -l ` != 1 ];
#      then MSG=`git log --format=oneline | head -1`
#      svn commit --password ${ASF_PASSWORD} ${document_folder_svn} -m "${MSG:41}"
#  else
#      echo "No need to refresh website.";
#  fi
}


####################################################
####################################################
# Script running start from here

prepare_release

if [[ "$RELEASE_STEP" == "reset" ]]; then
    reset_release
elif [[ "$RELEASE_STEP" == "publish-snapshot" ]]; then
    publish_snapshot_source
    publish_snapshot_package
elif [[ "$RELEASE_STEP" == "publish-release" ]]; then
    publish_release
elif [[ "$RELEASE_STEP" == "preview-site" ]]; then
    preview_site
elif [[ "$RELEASE_STEP" == "publish-site" ]]; then
    publish_site
fi