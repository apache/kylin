---
layout: docs-cn
title:  "windows加速构建Kylin源码 安装本地nexus"
categories: development
permalink: /cn/development/kylin_build_src_setup_windows_local_nexus.html
---
在构建Kylin源码过程中,会出现下载jar缓慢,或者是找不到jar等问题。
通过安装本地nexus来解决以上问题。

### windows加速构建Kylin源码 安装本地nexus
1. 下载nexus
https://www.sonatype.com/download-oss-sonatype
![Windos](/images/develop/kylin_build_src_setup_local_nexus01.png)
2. 解压安装包nexus-3.14.0-04-win64.zip
3.配置nexus的端口和上下文路径
- ../nexus-3.14.0-04-win64/nexus-3.14.0-04/etc/nexus-default.properties
```properties
	application-host : Nexus服务监听的主机
	application-port: Nexus服务监听的端口
	nexus-context-path : Nexus服务的上下文路径
```
4. 运行环境配置
-　打开解压目录下的 ../nexus-3.14.0-04-win64/nexus-3.14.0-04/bin/nexus.vmoptions
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

5. 安装nexus
- 在.../nexus-3.14.0-04-win64/nexus-3.14.0-04/bin 目录下，以管理员身份运行cmd
```cmd
nexus.exe /run 命令可以启动nexus服务
安装nexus本地服务来启动
nexus.exe /install <optional-service-name>　　//安装nexus服务
```

6. 启动/关闭nexus服务
- 在.../nexus-3.14.0-04-win64/nexus-3.14.0-04/bin 目录下，以管理员身份运行cmd 
```cmd
启动nexus服务
nexus.exe /start <optional-service-name> 
停止nexus服务  
nexus.exe /stop <optional-service-name>  
```

7.  登录
- http://localhost:prot/
- 默认的用户名和密码分别是：admin/amdin123

8. nexus仓库类型介绍
- 默认安装有以下这几个仓库，在控制台也可以修改远程仓库的地址，第三方仓库等。
|仓库名|作用|
|:----|:----|
|hosted（宿主仓库库）|存放本公司开发的jar包（正式版本、测试版本）|
|proxy（代理仓库）|代理中央仓库、Apache下测试版本的jar包|
|group（组仓库）|使用时连接组仓库，包含Hosted（宿主仓库）和Proxy（代理仓库）|
|virtual (虚拟仓库)|基本用不到，兼容Maven1版本的jar或者插件|


9. 在本地nexus添加需要使用的maven仓库地址
* 创建repository
![](/images/develop/kylin_build_src_setup_local_nexus02.png)
![](/images/develop/kylin_build_src_setup_local_nexus03.png)

* 添加Kyligence Maven仓库 https://repository.kyligence.io/repository/maven-public/
![](/images/develop/kylin_build_src_setup_local_nexus04.png)

* 添加CDH Maven仓库 https://repository.cloudera.com/artifactory/cdh-releases-rcs/
![](/images/develop/kylin_build_src_setup_local_nexus05.png)

* 添加阿里云 Maven仓库 https://maven.aliyun.com/repository/central/
![](/images/develop/kylin_build_src_setup_local_nexus06.png)


* 创建本地 nexus仓库
![](/images/develop/kylin_build_src_setup_local_nexus07.png)


10. 修改本地Maven setting.xml文件
```xml
      <mirror>
            <id>nexus</id>
            <name>local Maven</name>
            <mirrorOf>*</mirrorOf>
            <url>http://localhost:8081/repository/maven-public/</url>
      </mirror>
```

