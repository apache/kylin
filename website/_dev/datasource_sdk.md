---
layout: dev
title:  Develop JDBC Data Source
categories: development
permalink: /development/datasource_sdk.html
---

> Available since Apache Kylin v2.6.0

## Data source SDK

Since v2.6.0 Apache Kylin provides a new data source framework *Data source SDK*, which provides APIs to help developers handle dialect differences and easily implement a new data source. 

## How to develop

### Configuration to implement a new data source

*Data source SDK* provides a conversion framework and has pre-defined a configuration file *default.xml* for ansi sql dialect.

Developers do not need coding, what they should do is just create a new configuration file {dialect}.xml for the new data source dialect.

Structure of the configuration:

* Root node: 

```
<DATASOURCE_DEF NAME="kylin" ID="mysql" DIALECT="mysql"/>
```

The value of ID is normally the same with configuration file.  
The value of DIALECT is defined mainly for quote string for database identifier.  
For example Mysql use ``, Microsoft sql server use [].   
Mapping of Kylin DIALECT and Apache Calcite Dialect as belows:  
<table>
  <tbody align="left">  
  <tr>
    <td align="center">Dialect in Kylin</td>
    <td align="center">Dialect in Apache Calcite</td>
  </tr>
  <tr>
    <td> default </td>
    <td> SqlDialect.CALCITE </td>
  </tr>
  <tr>
    <td> calcite </td>
    <td> SqlDialect.CALCITE </td>
  </tr>
  <tr>
    <td> greenplum </td>
    <td> SqlDialect.DatabaseProduct.POSTGRESQL </td>
  </tr>
  <tr>
    <td> postgresql </td>
    <td> SqlDialect.DatabaseProduct.POSTGRESQL </td>
  </tr>
  <tr>
    <td> mysql  </td>
    <td> SqlDialect.DatabaseProduct.MYSQL </td>
  </tr>
  <tr>
     <td> sql.keyword-default-uppercase </td>
     <td> whether &lt;default&gt; should be transform to uppercase </td>
  </tr>
  <tr>
    <td> mssql </td>
    <td> SqlDialect.DatabaseProduct.MSSQL </td>
  </tr>
  <tr>
    <td> oracle </td>
    <td> SqlDialect.DatabaseProduct.ORACLE </td>
  </tr>
  <tr>
    <td> vertica </td>
    <td> SqlDialect.DatabaseProduct.VERTICA </td>
  </tr>
  <tr>
    <td> redshift </td>
    <td> SqlDialect.DatabaseProduct.REDSHIFT </td>
  </tr>
  <tr>
    <td> hive </td>
    <td> SqlDialect.DatabaseProduct.HIVE </td>
  </tr>
  <tr>
    <td> h2 </td>
    <td> SqlDialect.DatabaseProduct.H2 </td>
  </tr>
  <tr>
    <td> unkown </td>
    <td> SqlDialect.DUMMY </td>
  </tr>    
  </tbody>
</table>

* Property node:  
Define the properties of the dialect.

<table>
  <tbody align="left">  
  <tr>
    <td align="center">Property</td>
    <td align="center">Description</td>
  </tr>
  <tr>
    <td> sql.default-converted-enabled </td>
    <td> whether enable convert </td>
  </tr>
  <tr>
    <td> sql.allow-no-offset </td>
    <td> whether allow no offset </td>
  </tr>
  <tr>
    <td> sql.allow-fetch-no-rows </td>
    <td> whether allow fetch 0 rows </td>
  </tr>
  <tr>
    <td> sql.allow-no-orderby-with-fetch </td>
    <td> whether allow fetch without orderby </td>
  </tr>
  <tr>
    <td> sql.keyword-default-escape  </td>
    <td> whether &lt;default&gt; is keyword </td>
  </tr>
  <tr>
     <td> sql.keyword-default-uppercase </td>
     <td> whether &lt;default&gt; should be transform to uppercase </td>
  </tr>
  <tr>
    <td> sql.paging-type </td>
    <td> paging type like LIMIT_OFFSET, FETCH_NEXT, ROWNUM </td>
  </tr>
  <tr>
    <td> sql.case-sensitive </td>
    <td> whether identifier is case sensitive </td>
  </tr>
  <tr>
    <td> metadata.enable-cache </td>
    <td> whether enable cache for `sql.case-sensitive` is true </td>
  </tr>
  <tr>
    <td> sql.enable-quote-all-identifiers </td>
    <td> whether enable quote </td>
  </tr>
  <tr>
    <td> transaction.isolation-level </td>
    <td> transaction isolation level for sqoop </td>
  </tr>
  </tbody>
</table>


* Function node:  
Developers can define the functions implementation in target data source dialect.  
For example, we want to implement Greenplum as data source, but Greenplum does not support function such as *TIMESTAMPDIFF*, so we can define in *greenplum.xml* 

``` 
<FUNCTION_DEF ID="64" EXPRESSION="(CAST($1 AS DATE) - CAST($0 AS DATE))"/>
```

contrast with the configuration in *default.xml*

``` 
<FUNCTION_DEF ID="64" EXPRESSION="TIMESTAMPDIFF(day, $0, $1)"/>
```

*Data source SDK* provides conversion functions from default to target dialect with same function id.

* Type node:  
Developers can define the types implementation in target data source dialect.
Also take Greenplum as example, Greenplum support *BIGINT* instead of *LONG*, so we can define in *greenplum.xml*

``` 
<TYPE_DEF ID="Long" EXPRESSION="BIGINT"/>
```

contrast with the configuration in *default.xml*

``` 
<TYPE_DEF ID="Long" EXPRESSION="LONG"/>
```
*Data source SDK* provides conversion types from default to target dialect with same type id.


### Adaptor

Adaptor provides a list of API like get metadata and data from data source. 
*Data source SDK* provides a default implementation，developers can create a new class to extends it and have their own implementation.
{% highlight Groff markup %}
org.apache.kylin.sdk.datasource.adaptor.DefaultAdaptor
{% endhighlight %}

Adaptor also reserves a function *fixSql(String sql)*.  
After the conversion with the conversion framework, if the sql still have some problems to adapt the target dialect, developers can implement the function to fix sql finally. 


## How to enable data source for Kylin
Some new configurations:  
{% highlight Groff markup %}
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.pushdown.PushdownRunnerSDKImpl
kylin.source.default=16
kylin.source.jdbc.dialect={Dialect}
kylin.source.jdbc.adaptor={Class name of Adaptor}
kylin.source.jdbc.user={JDBC Connection Username}
kylin.source.jdbc.pass={JDBC Connection Password}
kylin.source.jdbc.connection-url={JDBC Connection String}
kylin.source.jdbc.driver={JDBC Driver Class Name}
{% endhighlight %}

Take mysql as an example:
{% highlight Groff markup %}
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.pushdown.PushdownRunnerSDKImpl
kylin.source.default=16
kylin.source.jdbc.dialect=mysql
kylin.source.jdbc.adaptor=org.apache.kylin.sdk.datasource.adaptor.MysqlAdaptor
kylin.source.jdbc.user={MYSQL_USERNAME}
kylin.source.jdbc.pass={MYSQL_PASSWORD}
kylin.source.jdbc.connection-url=jdbc:mysql://{HOST_URL}:3306/{DATABASE_NAME}
kylin.source.jdbc.driver=com.mysql.jdbc.Driver
{% endhighlight %}

Put the configuration file *{dialect}.xml* under directory $KYLIN_HOME/conf/datasource.
Create jar file for the new Adaptor, and put under directory $KYLIN_HOME/ext.

Other configurations are identical with the former jdbc connection, please refer to [setup_jdbc_datasource](/docs/tutorial/setup_jdbc_datasource.html)

