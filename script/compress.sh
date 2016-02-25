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
cd ${dir}/..

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
    echo "${version}"
fi

#package tar.gz
echo 'package tar.gz'
package_name=apache-kylin-${version}-bin
rm -rf dist/${package_name}
mkdir dist/${package_name}
# copy files
cp -r lib bin conf dist/tomcat examples/sample_cube dist/commit.sha1 dist/${package_name}
# copy license files
cp LICENSE README.md NOTICE dist/${package_name}

find dist/${package_name} -type d -exec chmod 755 {} \;
find dist/${package_name} -type f -exec chmod 644 {} \;
find dist/${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
cd dist/
tar -cvzf ${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

echo "Package ready dist/${package_name}.tar.gz"
