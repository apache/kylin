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

dir=$(dirname ${0})
cd ${dir}/../..
version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |  grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `

if [ "$version" == "" ];then
	echo "Failed to identify kylin version (current: `pwd`)"
	exit 1
fi

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

echo "package libraries"
mvn clean install -DskipTests	 || { exit 1; }

echo "copy libraries"
rm -rf build/lib
mkdir build/lib
cp assembly/target/kylin-assembly-${version}-job.jar build/lib/kylin-job-${version}.jar
cp storage-hbase/target/kylin-storage-hbase-${version}-coprocessor.jar build/lib/kylin-coprocessor-${version}.jar
cp jdbc/target/kylin-jdbc-${version}.jar build/lib/kylin-jdbc-${version}.jar
# Copied file becomes 000 for some env (e.g. my Cygwin)
chmod 644 build/lib/kylin-job-${version}.jar
chmod 644 build/lib/kylin-coprocessor-${version}.jar
chmod 644 build/lib/kylin-jdbc-${version}.jar

echo 'package tar.gz'
package_name=apache-kylin-${version}-lib
cd build/
rm -rf ${package_name}
mkdir ${package_name}
cp -r lib ${package_name}
rm -rf lib
find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
mkdir -p ../dist
tar -cvzf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

echo "Library package ready: dist/${package_name}.tar.gz"
