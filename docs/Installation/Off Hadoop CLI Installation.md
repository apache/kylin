Off Hadoop CLI Installation (Dev Env Setup)
===
Off-Hadoop-CLI installation is usually for **development use**.

Developers want to run kylin test cases or applications at their development machine. The scenario is depicted as:

![Off-Hadoop-CLI-installation](off_cli_install_scene.png)

The "Applications" here can be any unit cases running on your dev machine's IDE. By following this tutorial, you will be able to build kylin test cubes by running a specific test case, and you can further run other test cases against the cubes having been built.


## Environment on the Hadoop CLI

### Environment

Kylin Rquires a properly setup hadoop environment to run. Please take a look [this](Hadoop Environment.md).

## Environment on the dev machine

### Install maven

The latest maven can be found at <http://maven.apache.org/download.cgi>, we create a symbolic so that `mvn` can be run anywhere.

	cd ~
	wget http://apache.proserve.nl/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz
	tar -xzvf apache-maven-3.2.3-bin.tar.gz 
	ln -s /root/apache-maven-3.2.3/bin/mvn /usr/bin/mvn

### Compile

First clone the Kylin project to your local:

	git clone https://github.com/KylinOLAP/Kylin.git
	
Install Kylin artifacts to the maven repo

	mvn clean install -DskipTests

### Modify local configuration

Local configuration must be modified to point to your hadoop sandbox (or CLI) machine. If you are using a Hortonworks sandbox, this section may be skipped.

* In **examples/test_case_data/sandbox/kylin.properties**
   * Find `sandbox` and replace with your hadoop hosts
   * Find `kylin.job.remote.cli.username` and `kylin.job.remote.cli.password`, fill in the user name and password used to login hadoop cluster for hadoop command execution

* In **examples/test_case_data/sandbox**
   * For each configuration xml file, find all occurrence of `sandbox` and replace with your hadoop hosts

An alternative to the host replacement is updating your `hosts` file to resolve `sandbox` and `sandbox.hortonworks.com` to the IP of your sandbox machine.

### Run unit tests

Run a end-to-end cube building test
 
	mvn test -Dtest=org.apache.kylin.job.BuildCubeWithEngineTest -DfailIfNoTests=false
	
Run other tests, the end-to-end cube building test is exclueded

	mvn test

### Launch Kylin Web Server

In your Eclipse IDE, launch `org.apache.kylin.rest.DebugTomcat` with specifying VM arguments "-Dspring.profiles.active=sandbox". (By default Kylin server will listen on 7070 port; If you want to use another port, please specify it as a parameter when run `DebugTomcat)

Check Kylin Web available at http://localhost:7070/kylin (user:ADMIN,password:KYLIN)

