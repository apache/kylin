#!/usr/bin/env bash


cd ~
wget http://apache.cs.uu.nl/dist/tomcat/tomcat-7/v7.0.56/bin/apache-tomcat-7.0.56.tar.gz
tar -xzvf apache-tomcat-7.0.56.tar.gz
export CATALINA_HOME=/root/apache-tomcat-7.0.56

wget http://apache.proserve.nl/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz
tar -xzvf apache-maven-3.2.3-bin.tar.gz
ln -s /root/apache-maven-3.2.3/bin/mvn /usr/bin/mvn

wget http://nodejs.org/dist/v0.10.32/node-v0.10.32-linux-x64.tar.gz
tar -xzvf node-v0.10.32-linux-x64.tar.gz
export PATH=/root/node-v0.10.32-linux-x64/bin:$PATH