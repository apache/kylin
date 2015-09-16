#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

rm -rf build/tomcat

if [ ! -f "build/apache-tomcat-7.0.59.tar.gz" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-7/v7.0.59/bin/apache-tomcat-7.0.59.tar.gz || echo "download tomcat failed"
else
    if [ `md5sum apache-tomcat-7.0.59.tar.gz | awk '{print $1}'` != "ec570258976edf9a833cd88fd9220909" ]
    then
        echo "md5 check failed"
        rm apache-tomcat-7.0.59.tar.gz
        wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-7/v7.0.59/bin/apache-tomcat-7.0.59.tar.gz || echo "download tomcat failed"
    fi
fi

tar -zxvf build/apache-tomcat-7.0.59.tar.gz -C build/
mv build/apache-tomcat-7.0.59 build/tomcat
rm -rf build/tomcat/webapps/*

mv build/tomcat/conf/server.xml build/tomcat/conf/server.xml.bak
cp build/deploy/server.xml build/tomcat/conf/server.xml
echo "server.xml overwritten..."
