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

# pack webapp into kylin.war so that we have a all-in-one war
dir=$(dirname ${0})
cd ${dir}/../..

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |  grep -E '^[0-9]+\.[0-9]+\.[0-9]+'  `
    echo "${version}"
fi

#package tar.gz
echo 'package tar.gz'
package_name=apache-kylin-${version}-bin
cd build/
rm -rf ${package_name}
mkdir ${package_name}
cp -r ext lib tool bin conf tomcat ../examples/sample_cube commit_SHA1 VERSION ${package_name}
cp ../NOTICE ../LICENSE ../README.md ${package_name}
rm -rf ext lib tomcat commit_SHA1 VERSION

## comment all default properties, and append them to the user visible kylin.properties
## first 16 lines are license, just skip them
sed '1,16d' ../core-common/src/main/resources/kylin-defaults.properties | awk '{print "#"$0}' >> ${package_name}/conf/kylin.properties

find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;

mkdir -p ../dist
tar -cvzf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

echo "Package ready: dist/${package_name}.tar.gz"
