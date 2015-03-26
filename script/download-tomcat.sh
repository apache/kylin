#!/bin/sh

dir=$(dirname ${0})
cd ${dir}/..

rm -rf tomcat

if [ ! -f "apache-tomcat-7.0.59.tar.gz" ]
then
    echo "not binary file found"
    wget http://mirror.sdunix.com/apache/tomcat/tomcat-7/v7.0.59/bin/apache-tomcat-7.0.59.tar.gz || echo "download tomcat failed"
else
    if [ `md5sum apache-tomcat-7.0.59.tar.gz | awk '{print $1}'` != "ec570258976edf9a833cd88fd9220909" ]
    then
        echo "md5 check failed"
        rm apache-tomcat-7.0.59.tar.gz
        wget http://mirror.sdunix.com/apache/tomcat/tomcat-7/v7.0.59/bin/apache-tomcat-7.0.59.tar.gz || echo "download tomcat failed"
    fi
fi

tar -zxvf apache-tomcat-7.0.59.tar.gz
mv apache-tomcat-7.0.59 tomcat
rm -rf tomcat/webapps/*

mv tomcat/conf/server.xml tomcat/conf/server.xml.bak
cp deploy/server.xml tomcat/conf/server.xml
echo "server.xml overwritten..."
