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
  reset:
  preview-site:
  publish-site:

All other inputs are environment variables.

EOF
  exit 0
}

function info {
    cat << EOF
========================
|   $1
EOF
}

if [ $# -eq 0 ]; then
  exit_with_usage
else
  RELEASE_STEP=$1
  info "Running step : $RELEASE_STEP"
fi

function read_config {
  local PROMPT="$1"
  local DEFAULT="$2"
  local REPLY=

  read -p "$PROMPT [default is $DEFAULT]: " REPLY
  local RETVAL="${REPLY:-$DEFAULT}"
  if [ -z "$RETVAL" ]; then
    error "$PROMPT is must be provided."
  fi
  echo "$RETVAL"
}

function ask_confirm {
  read -p "$1. Will you continue? [y/n] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    info "Exiting."
    exit 1
  fi
}

function run_command {
  local BANNER="$1"
  shift 1

  echo "========================"
  echo "|   $BANNER"
  echo "Command: $@"

  "$@" 2>&1

  local EC=$?
  if [ $EC != 0 ]; then
    echo "Command FAILED : $@, please check!!!"
    exit $EC
  fi
}

ASF_USERNAME=$(read_config "Your apache id?" "")
GIT_USERNAME=$(read_config "Your full name(used as author of git commit)?" "Release manager")
ASF_PASSWORD=$(read_config "Your apache password?" "")
GIT_EMAIL=$ASF_PASSWORD"@apache.org"
GPG_KEY=$(read_config "GPG key of you(used to sign release candidate)?" "$GIT_EMAIL")
GPG_PASSPHRASE=$(read_config "PASSPHRASE for your private GPG key?" "")

GIT_BRANCH=$(read_config "Git branch for release?" "kylin5")
RELEASE_VERSION=$(read_config "Which version are you going to release?" "5.0")
NEXT_RELEASE_VERSION=$(read_config "Which version is the next development version?" "5.0")
RC_NUMBER="rc"$(read_config "Number for release candidate?" "1")

export working_dir=/root/apachekylin-release-folder
source_code_folder=$working_dir/source/kylin
packaging_folder=$source_code_folder/target/checkout
svn_stage_folder=$working_dir/svn/stage_repo
rc_name=apache-kylin-"${RELEASE_VERSION}"-${RC_NUMBER}
release_candidate_folder=$svn_stage_folder/$rc_name

branch_doc_1=document
branch_doc_2=doc5.0
doc_preview=preview
document_folder=$working_dir/document/src
document_folder_elder=$document_folder/$branch_doc_1
document_folder_newer=$document_folder/$branch_doc_2
document_folder_preview=$document_folder/$doc_preview

ASF_KYLIN_REPO="gitbox.apache.org/repos/asf/kylin.git"
# GITHUB_REPO_URL=${GIT_REPO_URL:-https://github.com/apache/kylin.git}
RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/kylin"
RELEASE_LOCATION="https://dist.apache.org/repos/dist/release/kylin"

GPG="gpg -u $GPG_KEY --no-tty --batch --pinentry-mode loopback"

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
  return 0
}

function prepare_release {
    info "Configuration and Clone Code"
    git config --global user.name "${GIT_USERNAME}"
    git config --global user.email "${ASF_USERNAME}"@apache.org
    git config --global user.password ${ASF_PASSWORD}
    mkdir -p $working_dir
    cd $working_dir

    if [ ! -d "${source_code_folder}" ]
    then
        info "Clone source code to ${source_code_folder} ."
        run_command "Clone Gitbox" git clone "https://$ASF_USERNAME:$ASF_PASSWORD@$ASF_KYLIN_REPO" -b "$GIT_BRANCH"
    fi

    if [ ! -d "${release_candidate_folder}" ]
    then
        info "Clone svn working dir to $working_dir ."
        run_command "Clone ASF SVN" svn co $RELEASE_STAGING_LOCATION $svn_stage_folder
    fi
    return 0
}

function publish_snapshot_source {
    info "Publish source code, maven artifact, git tag for release candidate"
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
    return 0
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
    run_command "Sign binary" gpg --armor --output apache-kylin-"${RELEASE_VERSION}"-bin.tar.gz.asc --detach-sig apache-kylin-${RELEASE_VERSION}-bin.tar.gz
    shasum -a 256 apache-kylin-"${RELEASE_VERSION}"-bin.tar.gz > apache-kylin-${RELEASE_VERSION}-bin.tar.gz.sha256


    ## Upload to svn repository
    ask_confirm "You are going to upload rc, are you sure you have the permissions?"
    cd ${svn_folder}
    svn add ${rc_name}
    run_command "Publish release candidate dir" svn commit -m 'Check in release artifacts for '${rc_name}
    return 0
}

function publish_release {
    info "Publish release candidate after vote succeed."
    # TODO
    return 0
}

####################################################
####################################################
# Script running start from here

prepare_release

if [[ "$RELEASE_STEP" == "reset" ]]; then
    reset_release
fi

if [[ "$RELEASE_STEP" == "publish-snapshot" ]]; then
    publish_snapshot_source
    publish_snapshot_package
fi

if [[ "$RELEASE_STEP" == "publish-release" ]]; then
    publish_release
fi

if [[ "$RELEASE_STEP" == "preview-site" ]]; then
    preview_site
fi


####################################################
####################################################
# Following is for website publish

function preview_site() {
    info "Prepare website"
    if [ ! -d "${documenst_folder}" ]; then
        mkdir -p $document_folder
        nvm install 16.14
    fi
    cd $document_folder
    if [ ! -d "${document_folder_elder}" ]; then
        git clone --branch $branch_doc_1 "https://$ASF_USERNAME:$ASF_PASSWORD@$ASF_KYLIN_REPO"
    else
        cd ${document_folder_elder}
        git reset --hard HEAD~4
        git pull -r origin $branch_doc_1
    fi

    if [ ! -d "${document_folder_newer}" ]; then
        git clone --branch $branch_doc_2 "https://$ASF_USERNAME:$ASF_PASSWORD@$ASF_KYLIN_REPO"
    else
        cd ${document_folder_newer}
        git reset --hard HEAD~4
        git pull -r origin $branch_doc_2
    fi

    if [ ! -d "${document_folder_preview}" ]; then
        mkdir ${document_folder_preview}
    else
        rm -rf full/*
    fi

    info "Build website"
    # TODO

    info "Website could be previewed at localhost:7070"
    return 0
}

function publish_site() {
    info "Publish website"
    # TODO
    return 0
}


####################################################
####################################################
# Following is for GPG Key

function fcreate_secure {
  local FPATH="$1"
  rm -f "$FPATH"
  touch "$FPATH"
  chmod 600 "$FPATH"
}

function import_gpg_key() {
  GPG="gpg --no-tty --batch"
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --import "$SELF/gpg.key"
  return 0
}