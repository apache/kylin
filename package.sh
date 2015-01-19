#!/bin/sh

mvn clean
mvn install -DskipTests

#Copy war to kylin.war
ls -t server/target/*.war| head -1 | awk '{print "cp " $1 " server/target/kylin.war"}' | sh
#Copy index jar
ls -t job/target/*-job.jar| head -1 | awk '{print "cp " $1 " job/target/kylin-job-latest.jar"}' | sh
#Copy query jar
ls -t storage/target/*-coprocessor.jar | head -1 | awk '{print "cp " $1 " storage/target/kylin-coprocessor-latest.jar"}' | sh

#package webapp
cd webapp/
npm install
npm install -g grunt-cli
grunt dev --buildEnv=dev
cd dist

# pack webapp into kylin.war so that we have a all-in-one war
for f in * .[^.]*
do
        echo "Adding $f to kylin.war"
        jar -uf ../../server/target/kylin.war $f
done