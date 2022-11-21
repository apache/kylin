---
title: Kylin JDBC Driver
language: en
sidebar_label: Kylin JDBC Driver
pagination_label: Kylin JDBC Driver
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - JDBC
draft: false
last_update:
    date: 11/21/2022
---

Kylin provides JDBC driver, which enables BI or other applications with JDBC interface to access Kylin instance.  

### How to get JDBC Driver

There are two methods to get JDBC Driver:

1. **(Recommended)** You can get Kylin JDBC driver from Kylin's installation directory's subdirectory `./lib` , and placed in the BI or other third party applications specified path.

2. You can get Kylin JDBC driver by executing the scripts from `${KYLIN CODES HOME}/build/release/jdbc_package.sh` to generate a full JDBC driver tar package and unzip it to get driver.

> **Note：**
>
> Support connecting to Kylin 5.0 and higher versions
>

### How to configure JDBC connection

Kylin JDBC driver follows the JDBC standard interface, users can specify the Kylin service to which the JDBC connection is made via the URL and the URL form is:

```
jdbc:kylin://<hostname>:<port>/<project_name>
```

URL parameter description is as follows:

- &lt;hostname&gt;: If Kylin service start SSL, then JDBC should use the HTTPS port of the Kylin service 

- &lt;port&gt;：If port has not been specified, then JDBC driver would use default port of HTTP and HTTPS 
- &lt;project_name&gt;:  Required. users have to ensure the project exist in Kylin service 



Besides, users need to specify username, password and whether SSL would be true for connection, these properties are as follow: 

- &lt;user&gt;: 	username to login Kylin service
- &lt;password&gt;: password to login Kylin service
- <ssl&gt;: enable ssl parameter. Set up string type "true" / "false", the default setting for this parameter  is "false". If the value is "true", all accesses to Kylin are based on HTTPS
- &lt;timeout&gt;: timeout in milliseconds for requests for Kylin. Default to 0 (no timeout)


Here lists an example of Connection: 

```java
Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
Properties info = new Properties();
info.put("user", "ADMIN");
info.put("password", "KYLIN");
//info.put("ssl","true");
Connection conn = driver.connect("jdbc:kylin://localhost:7070/kylin_project_name", info);
```



The following sections describe how two JAVA programs can connect to JDBC

#### Query based on Statement 
Here lists an example of Query based on Statement：
```java
Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
Properties info = new Properties();
info.put("user", "ADMIN");
info.put("password", "KYLIN");
//info.put("ssl","true");
Connection conn = driver.connect("jdbc:kylin://localhost:7070/kylin_project_name", info);
Statement state = conn.createStatement();
ResultSet resultSet = state.executeQuery("select * from test_table");
while (resultSet.next()) {
    System.out.println(resultSet.getString(1));
    System.out.println(resultSet.getString(2));
    System.out.println(resultSet.getString(3));
}
```


#### Query based on Prepared Statement 
Here lists an example of Query based on Prepared Statement： 

```java
Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
Properties info = new Properties();
info.put("user", "ADMIN");
info.put("password", "KYLIN");
//info.put("ssl","true");
Connection conn = driver.connect("jdbc:kylin://localhost:7070/kylin_project_name", info);
PreparedStatement state = conn.prepareStatement("select * from test_table where id=?");
state.setInt(1, 10);
ResultSet resultSet = state.executeQuery();
while (resultSet.next()) {
    System.out.println(resultSet.getString(1));
    System.out.println(resultSet.getString(2));
    System.out.println(resultSet.getString(3));
}
```

Among them, Prepared Statement supports assignment for the following types: 

* setString
* setInt
* setShort
* setLong
* setDouble
* setBoolean
* setByte
* setDate
* setTimestamp


**Prepared Statement Known Limitation**

- Query pushdown is not supported when using Prepared Statement.
- Dynamic param cannot follow with <b>'-'</b>, e.g. `SUM(price - ?)`
- Dynamic param cannot use in <b>case when</b>, e.g. `case when xxx then ? else ? end`

It's recommended to use dynamic param in filters only, e.g. `where id = ?`.

We also provide a way to bind dynamic parameter values. To enable this functionality, you need to add `kylin.query.replace-dynamic-params-enabled=true` in your kylin property file. 

Then the dynamic parameter limitations converges to:

- Column names and time units cannot be used as dynamic parameters.
- Type conversion functions `date` and `timestamp` are not supported, only `cast as` is supported.
- Some functions such as subtract_count support array parameter. The parameters in array also support dynamic parameters, such as `array['FP-GTC|FP-non GTC ', ?]`, but dynamic parameters are not supported in single quotes, such as `array['?|?', ?]`.

## JDBC User Delegation
In Kylin 5.0 and later version, you can use a authenticated user and delegate requests to another user by enabling Tableau Server to pass an Execute as user to the Kylin. As a result of this, the query will run on Kylin with the privileges of the Executed as user.

The following chapters introduce two ways of setting User Delegation,

#### Method one：JAVA Code

```java
Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
Properties info = new Properties();
info.put("user", "ADMIN");
info.put("password", "KYLIN");
info.put("EXECUTE_AS_USER_ID","EXECUTE_AS_NON_ADMIN");
Connection conn = driver.connect("jdbc:kylin://localhost:7070/kylin_project_name", info);
```

#### Method：Bind In Connect String

Bind the param in connect string:
`jdbc:kylin:EXECUTE_AS_USER_ID=EXECUTE_AS_NON_ADMIN;//localhost:7070/kylin_project_name` 

To make sure User Delegation is set successfully, you can observe this INFO level log in the JDBC log file: "Found the parameter EXECUTE_AS_USER_ID in the connection string. The query will be executed as the user defined in this connection string." And then the query user will be switched to EXECUTE_AS_NON_ADMIN.


## JDBC Driver Logging

You can enable logging in the driver to track activity and troubleshoot issues.

**Important:** Only enable logging long enough to capture an issue. Logging decreases performance and can consume a large quantity of disk space.

1. Open the driver logging configuration file in a text editor.
   For example, you would open the  {JDBC installed path}/kylin-jdbc.properties

   > **Note**：kylin-jdbc.properties is the default configuration file that needs to be placed in the same path as the JDBC jar package

3. Set log level. Information on all of the Log Levels is listed below.Trace is best in most cases.

   - **OFF** disables all logging.
   - **FATAL** logs very severe error events that might lead the driver to abort.
   - **ERROR**  logs error events that might still allow the driver to continue running.
   - **WARN**  logs potentially harmful situations.
   - **INFO**  logs general information that describes the progress of the driver.
   - **DEBUG**  logs detailed information that is useful for debugging the driver.
   - **TRACE** logs more detailed information than log level DEBUG.

   For example: **LogLevel=TRACE**

5. Set the Log Path and file name.Set the **LogPath** attribute to the full path to the folder where you want to save log files. This directory must exist and be writable, including being writable by other users if the application using the driver runs as a specific user.
   For example: **LogPath=/usr/local/KylinJDBC.log**      

6. Set the **MaxBackupIndex** attribute to the maximum number of log files to keep.
   For example: **MaxBackupIndex=10**

   > **Note**: After the maximum number of log files is reached, each time an additional file is created, the driver deletes the oldest file.

7. Set the **MaxFileSize** attribute to the maximum size of each log file in bytes.
   For example: **MaxFileSize=268435456**

   > **Note:** After the maximum file size is reached, the driver creates a new file and continues logging.

6. Save the driver configuration file.

   ```
   # Set log level
   LogLevel=TRACE
   
   # Set log path and file name
   LogPath=/usr/local/KylinJDBC.log
   
   # Set maximum number of log files to keep
   MaxBackupIndex=10
   
   # Set maximum size of each log file in bytes
   MaxFileSize=268435456
   ```

7. Restart the application you are using the driver with.Configuration changes will not be picked up by the application until it reloads the driver.

### FAQ

**Q: How to upgrade the JDBC Driver?** 

Remove the Kylin JDBC Driver package for BI or other third-party applications，and replace it with a new JDBC driver package.
