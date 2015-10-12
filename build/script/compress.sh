#!/bin/bash

# pack webapp into kylin.war so that we have a all-in-one war
dir=$(dirname ${0})
cd ${dir}/../..

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
    echo "${version}"
fi

#package tar.gz
echo 'package tar.gz'
package_name=apache-kylin-${version}-bin
cd build/
rm -rf ${package_name}
mkdir ${package_name}
cp -r lib bin conf tomcat ../examples/sample_cube commit_SHA1 ${package_name}
rm -rf lib tomcat commit_SHA1
find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
mkdir -p ../dist
tar -cvzf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

echo "Package ready: dist/${package_name}.tar.gz"
