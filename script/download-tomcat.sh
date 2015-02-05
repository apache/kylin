#!/bin/sh

dir=$(dirname ${0})
cd ${dir}/..

rm apache-tomcat-7.0.57.tar.gz
rm -rf tomcat
wget http://mirror.sdunix.com/apache/tomcat/tomcat-7/v7.0.57/bin/apache-tomcat-7.0.57.tar.gz

tar -zxvf apache-tomcat-7.0.57.tar.gz
mv apache-tomcat-7.0.57 tomcat