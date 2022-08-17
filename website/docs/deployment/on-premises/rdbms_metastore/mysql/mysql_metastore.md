---
title: Use MySQL as Metastore
language: en
sidebar_label: Use MySQL as Metastore
pagination_label: Use MySQL as Metastore
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
   - mysql
   - mysql metastore
   - metastore
draft: false
last_update:
   date: 08/11/2022
---


### <span id ="Prerequisite">Prerequisite</span>

1. Please install MySQL, If the MySQL has not been installed, please refer to [Insatall MySQL](install_mysql.md) for more details. 

### <span id ="ConfigurationSteps">Configuration Steps</span>

The following steps illustrate how to connect MySQL as metastore. Here is an example for MySQL 5.7 .

1. Create database `kylin` in MySQL

2. Set configuration item `kylin.metadata.url = {metadata_name}@jdbc` in `$KYLIN_HOME/conf/kylin.properties`,
   please replace `{metadata_name}` with your metadata name in MySQL, for example, `kylin_default_instance@jdbc`, the maximum length of `{metadata_name}` allowed is 29.

   > **Note**: If the metadata name doesn't exist, it will be automatically created in MySQL. Otherwise, Kylin will use the existing one.

   For example: 

   ```properties
   kylin.metadata.url={metadata_name}@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://{host}:{port}/kylin?useUnicode=true&characterEncoding=utf8,username={user},password={password},maxTotal=50,maxIdle=8
   ```
   
   The meaning of each parameter is as below, `url`, `username`, and `password` are required parameters. For others, default values will be used if they are not indicated.
	
   * **driverClassName**: JDBC's driver class name, default value is `com.mysql.jdbc.Driver`;
   * **url**: JDBC's url;
     * **host**：MySQL ip address, whose default value is `localhost`;
     * **port**：MySQL port, whose default value is `3306`. Please use the actual port to replace. 
     * **kylin**: Metabase name. Make sure this database `kylin` has been created in MySQL;
   * **username**: JDBC's username; 
   * **password**: JDBC's password;
   * **maxTotal**: max number of database's connection number, default value is 50; 
   * **maxIdle**: max number of database's waiting connection number, default value is 8;
   
> **Note**:  if your query SQL contains chinese, please configure the character encoding to utf8 in kylin.metadata.url to avoid confusing query history: useunicode = true & character encoding = utf8

3. Encrypt JDBC password

   If you need to encrypt JDBC's password, you can do it like this：
   
   **i.** run following commands in ${KYLIN_HOME}, it will print encrypted password
   ```shell
   ./bin/kylin.sh org.apache.kylin.tool.general.CryptTool -e AES -s <password>
   ```

   **ii.** config properties kylin.metadata.url's password like this
   ```properties
   password=ENC('${encrypted_password}')
   ```

   For example, the following assumes that the JDBC password is kylin:

   First, we need to encrypt kylin using the following command
   ```shell
   ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.general.CryptTool -e AES -s kylin
   AES encrypted password is: 
   YeqVr9MakSFbgxEec9sBwg==
   ```
   Then, config kylin.metadata.url like this：
   ```properties
   kylin.metadata.url={metadata_name}@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://{host}:{port}/kylin?useUnicode=true&characterEncoding=utf8,username={user},password=ENC('YeqVr9MakSFbgxEec9sBwg=='),maxTotal=20,maxIdle=20
   ```

4. If you need to use MySQL cluster deployment, please add `replication` or `loadbalance` in url with `"`. For example:

   ```properties
   #use replication in cluster deployment       
   kylin.metadata.url=kylin_default_instance@jdbc,driverClassName=com.mysql.jdbc.Driver,url="jdbc:mysql:replication://localhost:3306,localhost:3307/kylin?useUnicode=true&characterEncoding=utf8",username=root,password=,maxTotal=20,maxIdle=20
      
   #use loadbalance in cluster deployment
   kylin.metadata.url=kylin_default_instance@jdbc,driverClassName=com.mysql.jdbc.Driver,url="jdbc:mysql:loadbalance://localhost:3306,localhost:3307/kylin?useUnicode=true&characterEncoding=utf8",username=root,password=,maxTotal=20,maxIdle=20
   ```
5. Make sure that the storage engine used with MySQL is **InnoDB** is not **MyISAM** and that the default storage engine is modified as follows:

   ```Properties
   [mysqld]
   default-storage-engine=InnoDB
   ```

### <span id="faq">FAQ</span>

**Q: After the JDK is upgraded to jdk 8u261, the startup of Kylin fails, indicating that the creation of the admin user failed, what should I do? **

A: When you use JDK 8u261 and use MySQL 5.6 or 5.7 as metastore. Since the version before TLS 1.2 has been disabled since the JDK 8u261, and MySQL 5.6 and 5.7 use TLS 1.0 or TLS 1.1 by default, and MySQL must establish an SSL connection by default, which causes conflicts with the TLS protocol, resulting in the startup of Kylin fails, you will see the error message on the terminal as `Create Admin user failed`.

 You have 2 solutions:

Method 1: Modify the metadata configuration parameters and add `useSSL=false`

```properties
kylin.metadata.url={metadata_name}@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://{host}:{port}/kylin?useSSL=false,useUnicode=true&characterEncoding=utf8,username={user},password={password},maxTotal=50,maxIdle=8
```

Method 2: Modify the java security file `java.security`, find the following configuration, delete TLSv1, TLSv1.1

```sh
#  jdk.tls.disabledAlgorithms=MD5, SSLv3, DSA, RSA keySize < 2048
jdk.tls.disabledAlgorithms=SSLv3, TLSv1, TLSv1.1, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL, include jdk.disabled.namedCurves
```

