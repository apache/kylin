---
layout: docs-cn
title:  基于 MySQL 的 Metastore 配置
categories: 教程
permalink: /cn/docs/tutorial/mysql_metastore.html
since: v2.5.0
---

Kylin 支持 MySQL 作为 Metastore 存储。
> **注意**：该功能还在测试中，建议您谨慎使用*

### 准备工作

1.安装 MySQL 服务，例如 v5.1.17
2.下载 MySQL 的  JDBC 驱动 ( `mysql-connector-java-<version>.jar`) 并放置到 `$KYLIN_HOME/ext/` 目录下
> 提示：如果没有该目录，请自行创建。



### 配置方法
1.在 MySQL 中新建一个专为存储 Kylin 元数据的数据库，例如 `kylin_metadata`;
2.在配置文件 `kylin.properties` 中配置如下参数：

```properties
kylin.metadata.url={your_metadata_tablename}@jdbc,url=jdbc:mysql://localhost:3306/kylin,username={your_username},password={your_password}
kylin.metadata.jdbc.dialect=mysql
kylin.metadata.jdbc.json-always-small-cell=true
kylin.metadata.jdbc.small-cell-meta-size-warning-threshold=100mb
kylin.metadata.jdbc.small-cell-meta-size-error-threshold=1gb
kylin.metadata.jdbc.max-cell-size=1mb
```

`kylin.metadata.url` 中各配置项的含义如下，其中 `url`、`username` 和 `password` 为必须配置项，其他项如果不配置将使用默认值。

- `url`：JDBC 连接的 URL
- `username`：JDBC 的用户名
- `password`：JDBC 的密码，如果对密码进行了加密，填写加密后的密码
- `driverClassName`： JDBC 的 driver 类名，默认值为 com.mysql.jdbc.Driver
- `maxActive`：最大数据库连接数，默认值为 5
- `maxIdle`：最大等待中的连接数量，默认值为 5
- `maxWait`：最大等待连接毫秒数，默认值为 1000
- `removeAbandoned`：是否自动回收超时连接，默认值为 true
- `removeAbandonedTimeout`：超时时间秒数，默认为 300
- `passwordEncrypted`：是否对 JDBC 密码进行加密，默认为 false

3.如果需要对 JDBC 密码进行加密，请在 `$KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/`下运行如下命令：

```shell
java -classpath kylin-server-base-\<version\>.jar`kylin-core-common-\<version\>.jar`spring-beans-4.3.10.RELEASE.jar`spring-core-4.3.10.RELEASE.jar`commons-codec-1.7.jar org.apache.kylin.rest.security.PasswordPlaceholderConfigurer AES <your_password>
```

4.启动 Kylin