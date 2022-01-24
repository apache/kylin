#!/bin/bash
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

if [[ -z  "$KYLIN_TPCH_HOME" ]]; then
  dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
  export KYLIN_TPCH_HOME=`cd "${dir}/../"; pwd`
  logging info "KYLIN_TPCH_HOME not set, will use $KYLIN_TPCH_HOME as KYLIN_TPCH_HOME."
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
  if [[ ! -d $KYLIN_TPCH_HOME/venv ]]; then
    python3 -m venv $KYLIN_TPCH_HOME/venv
    source $KYLIN_TPCH_HOME/venv/bin/activate
    logging info "Install dependencies ..."
    pip3 install -r $KYLIN_TPCH_HOME/requirements.txt
  else
    logging warn "$KYLIN_TPCH_HOME/.venv already existing, skip install again."
  fi
  logging info "Please use 'source venv/bin/activate' to activate venv and execute commands."
  logging info "Please use 'python ./deploy --h[|--help]' to check the usage."
  logging info "Enjoy it and have fun."
}

function main {
    install_env
}

main
