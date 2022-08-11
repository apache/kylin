---
title: Use PostgreSQL as Metastore
language: en
sidebar_label: Use PostgreSQL as Metastore
pagination_label: Use PostgreSQL as Metastore
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
   - postgresql
draft: false
last_update:
   date: 11/08/2022
---

### <span id="preparation">Prerequisite</span>

1. For Kylin, we recommend using PostgreSQL as the default metastore database. The PostgreSQL 10.7 installation package is located in the  product package root directory `postgresql`.
2. If using other versions of PostgreSQL, please choose a version above PostgreSQL 9.1.
3. If you have not installed PostgreSQL, please check the [Install PostgreSQL](install_postgresql.md) chapter to complete the installation.



### <span id ="setting">Use PostgreSQL as metastore</span>

Next, we will introduce how to configure PostgreSQL as the metastore of Kylin.

1. Later, you can set the metadata url in the configuration file `$KYLIN_HOME/conf/kylin.properties`. The property is `kylin.metadata.url = {metadata_name}@jdbc`, please replace `{metadata_name}` with the table name you would like, for instance, `kylin_metadata@jdbc`, the maximum length of `{metadata_name}` allowed is 28. See the example below:

   ```properties
   kylin.metadata.url={metadata_name}@jdbc,driverClassName=org.postgresql.Driver,url=jdbc:postgresql://{host}:{port}/kylin,username={user},password={password}
   ```

   The meaning of each configuration item is as follows, `url`, `username` and `password` are required, other fields will use the default value if not set:

   - **url**: JDBC url:
     - **host**: The IP address of PostgreSQL server, the default value is `localhost`;
     - **port**: The port of PostgreSQL server, the default value is `5432`, you can set it with available port number.
     - **kylin**: Metabase name. Make sure this database `kylin` has been created in PostgreSQL;
   - **username**: JDBC user name, the default value is `postgres`;
   - **password**: JDBC password, the default value is void, please set it according to your actual password;
   - **driverClassName**: JDBC driver name, the default value is `org.postgresql.Driver`;

   **vi.** If you need to configure the cluster deployment, please use comma `,` to split among server addresses. Meanwhile, the url should use `"` to quote the url. For example:

   ```properties   	    
   kylin.metadata.url=kylin_metadata@jdbc,driverClassName=org.postgresql.Driver,url="jdbc:postgresql://{ip}:{port},{ip}:{port}.../kylin",username=postgres,password=kylin
   ```

2. If you need to encrypt JDBC's password, please follow undermentioned instructions：

   **i.** To obtain encrypted password, please run the command under the path of ${KYLIN_HOME}

   ```shell
   ./bin/kylin.sh org.apache.kylin.tool.general.CryptTool -e AES -s <password>
   ```

   **ii.** Configure the password in the `kylin.metadata.url` like this

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

   Then, configure `kylin.metadata.url` like this：

   ```properties
   kylin.metadata.url=kylin_metadata@jdbc,driverClassName=org.postgresql.Driver,url="jdbc:postgresql://{host}:{port},{ip}:{port}.../kylin",username=postgres,password=ENC('YeqVr9MakSFbgxEec9sBwg==')
   ```




