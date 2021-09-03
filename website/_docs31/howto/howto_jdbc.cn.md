---
layout: docs31-cn
title:  Kylin JDBC Driver
categories: 帮助
permalink: /cn/docs31/howto/howto_jdbc.html
---

### 认证

###### 基于Apache Kylin认证RESTFUL服务。支持的参数：
* user : 用户名
* password : 密码
* ssl: true或false。 默认为flas；如果为true，所有的服务调用都会使用https。

### 连接url格式：
{% highlight Groff markup %}
jdbc:kylin://<hostname>:<port>/<kylin_project_name>
{% endhighlight %}
* 如果“ssl”为true，“port”应该是Kylin server的HTTPS端口。
* 如果“port”未被指定，driver会使用默认的端口：HTTP 80，HTTPS 443。
* 必须指定“kylin_project_name”并且用户需要确保它在Kylin server上存在。

### 1. 使用Statement查询
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

### 2. 使用PreparedStatementv查询

###### 支持的PreparedStatement参数：
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

### 3. 获取查询结果元数据
Kylin jdbc driver支持元数据列表方法：
通过sql模式过滤器（比如 %）列出catalog、schema、table和column。

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
