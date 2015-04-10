#!/bin/sh

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
rm -rf kylin-${version}
mkdir kylin-${version}
cp -r lib bin conf tomcat examples/sample_cube kylin-${version}
find kylin-${version} -type d -exec chmod 755 {} \;
find kylin-${version} -type f -exec chmod 644 {} \;
find kylin-${version} -type f -name "*.sh" -exec chmod 755 {} \;
tar -cvzf kylin-${version}.tar.gz kylin-${version}
rm -rf kylin-${version}

echo "Package ready kylin-${version}.tar.gz"
