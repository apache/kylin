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

echo "Checking maven..."

if [ -z "$(command -v mvn)" ]
then
    echo "Please install maven first so that Kylin packaging can proceed"
    exit 1
else
    echo "maven check passed"
fi

echo "Checking git..."

if [ -z "$(command -v git)" ]
then
    echo "Please install git first so that Kylin packaging can proceed"
    exit 1
else
    echo "git check passed"
fi


echo "Checking npm..."

if [ -z "$(command -v npm)" ]
then
    echo "Please install npm first so that Kylin packaging can proceed"
    exit 1
else
    echo "npm check passed"
fi

dir=$(dirname ${0})
cd ${dir}/../..
version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |  grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
echo "kylin version: ${version}"
export version

#commit id
cat << EOF > build/commit_SHA1
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
EOF
git rev-parse HEAD >> build/commit_SHA1

sh build/script/build.sh $@         || { exit 1; }
sh build/script/download-tomcat.sh  || { exit 1; }
sh build/script/download-spark.sh   || { exit 1; }
sh build/script/prepare.sh          || { exit 1; }
sh build/script/compress.sh         || { exit 1; }
