---
layout: docs31-cn
title:  基于 MySQL 的 Metastore 配置
categories: 教程
permalink: /cn/docs31/tutorial/mysql_metastore.html
since: v2.5.0
---

Kylin 支持 MySQL 作为 Metastore 存储。

> **注意**：该功能还在测试中，建议您谨慎使用。



### 准备工作

1.安装 MySQL 服务，例如 v5.1.17
2.下载 MySQL 的  JDBC 驱动 ( `mysql-connector-java-<version>.jar`) 并放置到 `$KYLIN_HOME/ext/` 目录下。

> 提示：如果没有该目录，请自行创建。



### 配置方法

1.在 MySQL 中新建一个专为存储 Kylin 元数据的数据库，例如 `kylin`;

2.在配置文件 `kylin.properties` 中配置 `kylin.metadata.url={metadata_name}@jdbc`，该参数中各配置项的含义如下，其中 `url`、`username` 和 `password` 为必须配置项，其他项如果不配置将使用默认值。

> 提示：{metadata_name} 需要替换成用户需要的元数据表名，如果这张表已存在，会使用现有的表；如果不存在，则会自动创建该表。

- `url`：JDBC 连接的 URL
- `username`：JDBC 的用户名
- `password`：JDBC 的密码，如果对密码进行了加密，填写加密后的密码
- `driverClassName`： JDBC 的 driver 类名，默认值为 `com.mysql.jdbc.Driver`
- `maxActive`：最大数据库连接数，默认值为 5
- `maxIdle`：最大等待中的连接数量，默认值为 5
- `maxWait`：最大等待连接毫秒数，默认值为 1000
- `removeAbandoned`：是否自动回收超时连接，默认值为 true
- `removeAbandonedTimeout`：超时时间秒数，默认为 300
- `passwordEncrypted`：是否对 JDBC 密码进行加密，默认为 FALSE

> 提示：如果需要对 JDBC 密码进行加密，请在 `$KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/`下运行如下命令：

```sh
java -classpath kylin-server-base-<version>.jar\
:kylin-core-common-<version>.jar\
:spring-beans-4.3.10.RELEASE.jar:spring-core-4.3.10.RELEASE.jar\
:commons-codec-1.7.jar \
org.apache.kylin.rest.security.PasswordPlaceholderConfigurer \
AES <your_password>
```

如在 Kylin v2.5 中，执行如下命令：

```sh
java -classpath kylin-server-base-2.5.0.jar\
:kylin-core-common-2.5.0.jar\
:spring-beans-4.3.10.RELEASE.jar\
:spring-core-4.3.10.RELEASE.jar\
:commons-codec-1.7.jar \
org.apache.kylin.rest.security.PasswordPlaceholderConfigurer \
AES test123
```

执行结果如下：

```
AES encrypted password is:
bUmSqT/opyqz89Geu0yQ3g==
```
将生成的密码填入 `kylin.metadata.url` 中的 `password` 中，设置 `passwordEncrypted` 为 TRUE。

3.由于元数据不依赖于 HBase，所以需要在配置文件 `$KYLIN_HOME/conf/kylin.properties` 中添加 ZooKeeper 的连接项 `kylin.env.zookeeper-connect-string = host:port`。

4.`kylin.properties` 的样例配置如下：

```properties
kylin.metadata.url=mysql_test@jdbc,url=jdbc:mysql://localhost:3306/kylin,username=kylin_test,password=bUmSqT/opyqz89Geu0yQ3g==,maxActive=10,maxIdle=10,passwordEncrypted=true
kylin.env.zookeeper-connect-string=localhost:2181
```

5.启动 Kylin

