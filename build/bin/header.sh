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

# source me

verbose=${verbose:-""}

while getopts ":v" opt; do
    case $opt in
        v)
            echo "Turn on verbose mode." >&2
            verbose=true
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            ;;
    esac
done

if [[ "$dir" == "" ]]
then
	dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
	
	# set KYLIN_HOME with consideration for multiple instances that are on the same node
	KYLIN_HOME=${KYLIN_HOME:-"${dir}/../"}
	export KYLIN_HOME=`cd "$KYLIN_HOME"; pwd`
	dir="$KYLIN_HOME/bin"
	
	function quit {
		echo "$@"
		exit 1
	}
	
	function verbose {
		if [[ -n "$verbose" ]]; then
			echo "$@"
		fi
	}
fi
