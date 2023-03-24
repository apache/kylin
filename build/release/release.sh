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

dir=$(dirname ${0})
cd ${dir}/../..
source build/release/functions.sh

export WITH_SPARK=1
export WITH_HIVE1=1
export WITH_THIRDPARTY=0
export WITH_FRONT=1

export PACKAGE_OFFICIAL=0
export SKIP_COMPILE=0

for PARAM in $@; do
    if [[ "$PARAM" == "-official" ]]; then
        echo "Package for official release..."
        export PACKAGE_OFFICIAL=1
        shift
    fi

    if [[ "$PARAM" == "-noSpark" ]]; then
        echo "Skip packaging Spark..."
        export WITH_SPARK=0
        shift
    fi

    if [[ "$PARAM" == "-noHive1" ]]; then
        echo "Package without Hive 1.2.2..."
        export WITH_HIVE1=0
        shift
    fi

    if [[ "$PARAM" == "-withThirdParty" ]]; then
        echo "Package with Third Party..."
        export WITH_THIRDPARTY=1
        shift
    fi

    if [[ "$PARAM" == "-skipFront" ]]; then
        echo 'Skip install front-end dependencies...'
        export WITH_FRONT=0
        shift
    fi
    if [[ "$PARAM" == "-skipCompile" ]]; then
        echo 'Skip install backend-end dependencies...'
        export SKIP_COMPILE=1
        shift
    fi
done

## Fetch kylin version from pom
exportProjectVersions

if [[ -z ${release_version} ]]; then
    release_version='staging'
fi
if [[ "${PACKAGE_OFFICIAL}" = "0" ]]; then
    timestamp=`date '+%Y%m%d%H%M%S'`
    export release_version=${release_version}.${timestamp}
fi
export package_name="apache-kylin-${release_version}-bin"

bash build/release/package.sh $@ || { echo "package failed!"; exit 1; }

echo "Release Version: ${release_version}"

package_name="apache-kylin-${release_version}-bin.tar.gz"
sha256sum dist/$package_name > dist/${package_name}.sha256sum

echo "sha256: `cat dist/${package_name}.sha256sum`"
