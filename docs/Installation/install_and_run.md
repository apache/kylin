##Install and Run

###How to run

1. Download the release version(according to the hadoop distribution)
2. Setup a KYLIN_HOME pointing to the corresponding directory where you extract the release tar
3. Make sure the user has the privilege to run hadoop, hive and hbase cmd in shell. If you are not so sure, you can just run **bin/check-env.sh**, it will print out the detail information if you have some environment issues.
4. To start Kylin, simply run **bin/kylin.sh start**
5. To stop Kylin, simply run **bin/kylin.sh stop**


If you are running Kylin in a cluster or you have multiple Kylin instances, please make sure you have the following property correctly configured.

1. kylin.rest.servers 

	List of web servers in use, this enables one web server instance to sync up with other servers.
  

2. kylin.server.mode

	Make sure there is only one instance whose "kylin.server.mode" is set to "all" if there are multiple instances.
	

###The directory structure of the installation

>     ├── bin
>     │   ├── check-env.sh
>     │   ├── find-hive-dependency.sh
>     │   ├── health-check.sh
>     │   ├── kylin.sh
>     │   └── sample.sh
>     │
>     ├── conf
>     │   ├── kylin_job_conf.xml
>     │   └── kylin.properties
>     │
>     ├── sample_cube
>     │   ├── data
>     │   └── metadata
>     │
>     ├── tomcat
>     │   ├── webapps
>     │   │    └── kylin.war
>     │    …….
>     │ 
>     └── lib
>         ├── kylin-coprocessor-${version}.jar
>         ├── kylin-job-${version}.jar
>         └── kylin-jdbc-${version}.jar

###Create sample cube
Kylin provides a script for you to create a sample Cube; the script will also create three sample hive tables:

1. Run ${KYLIN_HOME}/bin/sample.sh
2. Logon Kylin web, select project "learn_kylin";
3. Select the sample cube "kylin_sales_cube", click "Actions" -> "Build", pick up a date later than 2014-01-01 (to cover all 10000 sample records);
4. Check the build progress in "Jobs" tab, until 100%;
5. Execute SQLs in the "Query" tab, for example:
	select cal_dt, sum(price) as total_selled, count(distinct seller_id) as sellers from kylin_sales group by cal_dt order by cal_dt
6. You can verify the query result and compare the response time with hive;

###Build from source
>     git clone https://github.com/KylinOLAP/Kylin.git   
>     cd KylinOLAP/Kylin   
>     sh script/package.sh

In order to generate binary package, **maven** and **npm** are pre-requisites.
















