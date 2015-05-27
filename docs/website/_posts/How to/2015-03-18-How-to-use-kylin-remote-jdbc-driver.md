---
layout: post
title:  "How to use kylin remote jdbc driver"
date:   2015-03-18
author: SHAOFENG SHI
categories: howto
---

### Authentication

###### Build on kylin authentication restful service. Supported parameters:
* user : username 
* password : password
* ssl: true/false. Default be false; If true, all the services call will use https.

### Connection URL format:
{% highlight Groff markup %}
jdbc:kylin://<hostname>:<port>/<kylin_project_name>
{% endhighlight %}
* If "ssl" = true, the "port" should be Kylin server's HTTPS port; 
* If "port" is not specified, the driver will use default port: HTTP 80, HTTPS 443;
* The "kylin_project_name" must be specified and user need ensure it exists in Kylin server;

### 1. Query with Statement
{% highlight Groff markup %}
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();

        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://localhost:7070/kylin_project_name", info);
        Statement state = conn.createStatement();
        ResultSet resultSet = state.executeQuery("select * from test_table");

        while (resultSet.next()) {
            assertEquals("foo", resultSet.getString(1));
            assertEquals("bar", resultSet.getString(2));
            assertEquals("tool", resultSet.getString(3));
        }
{% endhighlight %}

### 2. Query with PreparedStatement

###### Supported prepared statement parameters:
* setString
* setInt
* setShort
* setLong
* setFloat
* setDouble
* setBoolean
* setByte
* setDate
* setTime
* setTimestamp

{% highlight Groff markup %}
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://localhost:7070/kylin_project_name", info);
        PreparedStatement state = conn.prepareStatement("select * from test_table where id=?");
        state.setInt(1, 10);
        ResultSet resultSet = state.executeQuery();

        while (resultSet.next()) {
            assertEquals("foo", resultSet.getString(1));
            assertEquals("bar", resultSet.getString(2));
            assertEquals("tool", resultSet.getString(3));
        }
{% endhighlight %}

### 3. Get query result metadata
Kylin jdbc driver supports metadata list methods:
List catalog, schema, table and column with sql pattern filters(such as %).

{% highlight Groff markup %}
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://localhost:7070/kylin_project_name", info);
        Statement state = conn.createStatement();
        ResultSet resultSet = state.executeQuery("select * from test_table");

        ResultSet tables = conn.getMetaData().getTables(null, null, "dummy", null);
        while (tables.next()) {
            for (int i = 0; i < 10; i++) {
                assertEquals("dummy", tables.getString(i + 1));
            }
        }
{% endhighlight %}
