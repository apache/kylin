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

set -e

function info() {
    echo -e "\033[32m$@\033[0m"
}

function warn() {
    echo -e "\033[33m$@\033[0m"
}

function error() {
    echo -e "\033[31m$@\033[0m"
}

function logging() {
    case $1 in
        "info") shift; info $@ ;;
        "warn") shift; warn $@ ;;
        "error") shift; error $@ ;;
        *) echo -e $@ ;;
    esac
}

if [[ -z  "$TOOL_HOME" ]]; then
  dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
  export TOOL_HOME=`cd "${dir}/../"; pwd`
  logging info "TOOL_HOME not set, will use $TOOL_HOME as TOOL_HOME."
fi

function check_python_version {
  pyv=$(python3 --version 2>&1)
  currentver=${pyv:7}
  logging info "Current Python version is $currentver."

  requiredver=3.6.6

  if [[ "$(printf '%s\n' "$requiredver" "$currentver" | sort -V | head -n1)" != "$requiredver" ]]; then
    logging error "${currentver} less than required Python version ${requiredver}."
    exit 0
  fi
}

function install_env {
  check_python_version
  if [[ ! -d $TOOL_HOME/venv ]]; then
    python3 -m venv $TOOL_HOME/venv
    source $TOOL_HOME/venv/bin/activate
    logging info "Install dependencies ..."
    pip3 install -r $TOOL_HOME/requirements.txt
  else
    logging warn "$TOOL_HOME/.venv already existing, skip install again."
  fi
  logging info "Please use 'source venv/bin/activate' to activate venv and execute commands."
  logging info "Please use 'python deploy.py --help' to check the usage."
  logging info "Enjoy it and have fun."
}

function main {
    install_env
}

main
