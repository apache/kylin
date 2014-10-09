pwd

mvn clean
mvn install -DskipTests

#Copy war to kylin.war
ls -lt server/target/*.war| head -1 | awk '{print "cp " $9 " server/target/kylin.war"}' | sh
#Copy index jar
ls -lt job/target/*-job.jar| head -1 | awk '{print "cp " $9 " job/target/kylin-job-latest.jar"}' | sh
#Copy query jar
ls -lt storage/target/*-coprocessor.jar | head -1 | awk '{print "cp " $9 " storage/target/kylin-coprocessor-latest.jar"}' | sh

#package webapp
cd webapp/
npm install
grunt dev --buildEnv=dev
cd dist
tar -cvf Web.tar * .htaccess
