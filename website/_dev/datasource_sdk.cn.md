---
layout: dev
title:  开发 JDBC 数据源
categories: development
permalink: /cn/development/datasource_sdk.html
---

> 自 Apache Kylin v2.6.0 起有效

## Data source SDK
自 Apache Kylin v2.6.0 起，我们提供一套新的数据源框架 *Data source SDK*，使用框架提供的API, 开发者可以很轻松实现一个新的数据源, 适配sql方言。 

## 如何开发

### 实现新数据源的配置

*Data source SDK* 提供转换的机制, 框架里预定义一个配置文件 *default.xml* 对应ansi sql方言。

开发者不需要编码, 只需要为新的数据源新建一个配置文件 *{dialect}.xml*, 

配置文件结构:
* 根节点:  
&lt;DATASOURCE_DEF NAME="kylin" ID="default"&gt;, ID的值为方言的名称.
* 属性节点:  
定义方言的属性。

<table>
  <tbody align="left">  
  <tr>
    <td align="center">属性</td>
    <td align="center">描述</td>
  </tr>
  <tr>
    <td> sql.default-converted-enabled </td>
    <td> 是否需要转换 </td>
  </tr>
  <tr>
    <td> sql.allow-no-offset </td>
    <td> 是否允许没有offset字句 </td>
  </tr>
  <tr>
    <td> sql.allow-fetch-no-rows </td>
    <td> 是否允许fetch 0 rows</td>
  </tr>
  <tr>
    <td> sql.allow-no-orderby-with-fetch </td>
    <td> fetch是否必须跟orderby </td>
  </tr>
  <tr>
    <td> sql.keyword-default-escape  </td>
    <td> &lt;default&gt;是否是关键字 </td>
  </tr>
  <tr>
     <td> sql.keyword-default-uppercase </td>
     <td> &lt;default&gt; 是否需要转换成大写 </td>
  </tr>
  <tr>
    <td> sql.paging-type </td>
    <td> 分页类型比如 LIMIT_OFFSET, FETCH_NEXT, ROWNUM </td>
  </tr>
  <tr>
    <td> sql.case-sensitive </td>
    <td> 是否大小写敏感 </td>
  </tr>
  <tr>
    <td> metadata.enable-cache </td>
    <td> 是否开启缓存(针对开启大小写敏感) </td>
  </tr>
  <tr>
    <td> sql.enable-quote-all-identifiers </td>
    <td> 是否开启quote </td>
  </tr>
  <tr>
    <td> transaction.isolation-level </td>
    <td> 事务隔离级别(针对sqoop) </td>
  </tr>
  </tbody>
</table>


* 方法节点:  
开发者可以根据数据源方言定义方法的实现。
比如，我们想要实现 Greenplum 作为数据源，但是 Greenplum 不支持 *TIMESTAMPDIFF* 方法，那我们就可以在 *greenplum.xml* 里面定义 

```
<FUNCTION_DEF ID="64" EXPRESSION="(CAST($1 AS DATE) - CAST($0 AS DATE))"/>
```

对比在 *default.xml* 定义

```
<FUNCTION_DEF ID="64" EXPRESSION="TIMESTAMPDIFF(day, $0, $1)"/>
```

*Data source SDK* 可以把 default 里定义相同 function id 方法转换成目标方言里的定义.

* 类型节点:  
开发者可以根据数据源方言定义数据类型。
还是拿 Greenplum 作为例子, Greenplum 支持 *BIGINT* 而不是 *LONG*, 那我们可以在 *greenplum.xml* 定义

```
<TYPE_DEF ID="Long" EXPRESSION="BIGINT"/>
```

对比在 *default.xml* 定义

```
<TYPE_DEF ID="Long" EXPRESSION="LONG"/>
```

*Data source SDK* 可以把 default 里定义相同 type id 方法转换成目标方言里的定义.


### Adaptor

Adaptor 提供一系列的 API 比如从数据源获取元数据，数据等。  
*Data source SDK* 提供了默认的实现，开发者可以创建一个类继承它，并且有自己的实现。
{% highlight Groff markup %}
org.apache.kylin.sdk.datasource.adaptor.DefaultAdaptor
{% endhighlight %}
Adaptor 还预留一个方法 *fixSql(String sql)*. 
如果根据配置文件转换之后的 sql 还是和目标方言有些适配问题, 开发者可以去实现这个方法做 sql 最后的修改. 


## 部署
一些新的配置：  
{% highlight Groff markup %}
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.pushdown.PushdownRunnerSDKImpl
kylin.source.default=16
kylin.source.jdbc.dialect={JDBC 方言}
kylin.source.jdbc.adaptor={JDBC 连接的数据源对应的适配器类名}
kylin.source.jdbc.user={JDBC 连接用户名}
kylin.source.jdbc.pass={JDBC 连接密码}
kylin.source.jdbc.connection-url={JDBC 连接字符串}
kylin.source.jdbc.driver={JDBC 驱动类名}
{% endhighlight %}  

使用mysql作为例子:
{% highlight Groff markup %}
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.pushdown.PushdownRunnerSDKImpl
kylin.source.default=16
kylin.source.jdbc.dialect=mysql
kylin.source.jdbc.adaptor=org.apache.kylin.sdk.datasource.adaptor.MysqlAdaptor
kylin.source.jdbc.user={mysql username}
kylin.source.jdbc.pass={mysql password}
kylin.source.jdbc.connection-url=jdbc:mysql://{HOST_URL}:3306/{Database name}
kylin.source.jdbc.driver=com.mysql.jdbc.Driver
{% endhighlight %}

新增加的 *{dialect}.xml* 放置在 $KYLIN_HOME/conf/datasource 目录下。
新开发的 Adaptor 打成jar包后 放置在在 $KYLIN_HOME/ext 目录下。

其余的配置和更早的jdbc连接方式一致，请参考 [setup_jdbc_datasource](/cn/docs/tutorial/setup_jdbc_datasource.html)

