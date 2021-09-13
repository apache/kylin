---
layout: docs
title:  "Windows accelerated build Kylin source install native Nexus"
categories: development
permalink: /docs/development/kylin_build_src_setup_windows_local_nexus.html
---
When building Kylin's source code, there are problems such as slow jar downloads or jar discovery.  
Fix this by installing a local Nexus.  

### Windows accelerated build Kylin source install native Nexus
1. Download nexus
https://www.sonatype.com/download-oss-sonatype
![Windos](/images/develop/kylin_build_src_setup_local_nexus01.png)
2. Decompress the installation package nexus-3.14.0-04-win64.zip
3.Configure the port and context path for the Nexus
- ../nexus-3.14.0-04-win64/nexus-3.14.0-04/etc/nexus-default.properties
```properties
	application-host : The host on which the Nexus service listens
	application-port:  The port on which the Nexus service listens
	nexus-context-path : Context path of the Nexus service
```
4. Operating Environment Configuration
-　Open the decompression directory ../nexus-3.14.0-04-win64/nexus-3.14.0-04/bin/nexus.vmoptions
```properties
-Xms2703m
-Xmx2703m
-XX:MaxDirectMemorySize=2703m
-XX:+UnlockDiagnosticVMOptions
-XX:+LogVMOutput
-XX:LogFile=D:\path\nexus-3.28.1-01\sonatype-work\nexus3\log\jvm.log
-XX:-OmitStackTraceInFastThrow
-Djava.net.preferIPv4Stack=true
-Dkaraf.home=.
-Dkaraf.base=.
-Dkaraf.etc=etc/karaf
-Djava.util.logging.config.file=etc/karaf/java.util.logging.properties
-Dkaraf.data=D:\path\nexus-3.28.1-01\sonatype-work\nexus3
-Dkaraf.log=D:\path\nexus-3.28.1-01\sonatype-work\nexus3\log
-Djava.io.tmpdir=D:\path\nexus-3.28.1-01\sonatype-work\nexus3\tmp
-Dkaraf.startLocalConsole=false
```

5. Install nexus
- .../nexus-3.14.0-04-win64/nexus-3.14.0-04/bin,Run CMD as an administrator
```cmd
nexus.exe /run 命令可以启动nexus服务
安装nexus本地服务来启动
nexus.exe /install <optional-service-name>　　//安装nexus服务
```

6. Start/Stop nexus
- .../nexus-3.14.0-04-win64/nexus-3.14.0-04/bin，Run CMD as an administrator
```cmd
Start nexus
nexus.exe /start <optional-service-name> 
Stop nexus  
nexus.exe /stop <optional-service-name>  
```

7.  Login
- http://localhost:prot/
- The default user name and password are respectively：admin/amdin123

8. Nexus Warehouse type introduction  
- The following repositories are installed by default. You can also modify the address of remote warehouses, third-party warehouses, etc.  
|warehouse|effect|
|:----|:----|
|hosted（Host repository）|Store jar packages developed by our company (official version, test version)|
|proxy（The agent warehouse）|Proxy central repository, Apache under the test version of the JAR package|
|group（Set of warehouse）|Connect to a group repository when used, including Hosted repository and Proxy  |
|virtual (Virtual warehouse)|Basic use, compatible with Maven1 version of jar or plug-in|


9. Add the maven repository address you want to use in the local Nexus
* Create Repository
![](/images/develop/kylin_build_src_setup_local_nexus02.png)
![](/images/develop/kylin_build_src_setup_local_nexus03.png)

* ADD Kyligence Maven Repository https://repository.kyligence.io/repository/maven-public/
![](/images/develop/kylin_build_src_setup_local_nexus04.png)

* ADD CDH Maven Repository https://repository.cloudera.com/artifactory/cdh-releases-rcs/
![](/images/develop/kylin_build_src_setup_local_nexus05.png)

* ADD AliYun Maven Repository https://maven.aliyun.com/repository/central/
![](/images/develop/kylin_build_src_setup_local_nexus06.png)


* Create Local Nexus Repository
![](/images/develop/kylin_build_src_setup_local_nexus07.png)


10. Modifying Native  Maven's setting.xml文件
```xml
      <mirror>
            <id>nexus</id>
            <name>local Maven</name>
            <mirrorOf>*</mirrorOf>
            <url>http://localhost:8081/repository/maven-public/</url>
      </mirror>
```

