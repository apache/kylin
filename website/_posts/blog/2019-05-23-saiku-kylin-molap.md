---
layout: post-blog
title:  Saiku + Kylin 搭建多维 OLAP 平台
date:   2019-05-23 15:00:00
author: Gina Zhai
categories: blog
---

随着数据量的激增，传统的 OLTP 平台已无法满足用户的分析需求。OLAP 平台应运而生，OLAP 平台通常由 OLAP Engine 与用户操作分析页面组成。本文将会以 Saiku + Kylin 的组合方式讲解多维 OLAP 平台的搭建。

### Saiku 简介
Saiku 是一个用于 OLAP 分析的套件。用户可在浏览器中通过拖拽的方式进行分析。降低了使用者的学习成本。它能够连接多种数据源，如 Microsoft SQL Server, Oracle Database, MySQL, MongoDB 等。
Saiku 支持两种连接方式：一是 Mondrian，二是 XMLA。
Mondrian 是一个开源的商务分析引擎。使用 MDX(MultiDimensionalExpressions) 完成多维查询。
XMLA（XML for Analysis）是一种基于简单对象访问协议（SOAP）的 XML 协议。

### Kylin 简介
Kylin 是一个基于 Hadoop 的 OLAP 引擎，可查询分析历史与实时数据，并在亚秒级返回查询结果。

### 前期准备
- 安装 JAVA 并设置 JAVA_HOME
- 从 [Saiku 社区](https://community.meteorite.bi/)下载 Saiku 最新版本
- 从 [Kylin 官网](http://kylin.apache.org/download/)下载 Kylin 最新版本，即 2.6.2
- 在 Kylin v2.6.2 中构建一个 Cube

### 连接 Saiku 到 Kylin
1.执行 `unzip saiku-latest.zip` 解压下载好的 Saiku 包，切换至 saiku-server 文件夹。解压下载好的 Kylin 包，进入解压后文件夹，将 lib/kylin-jdbc-<version>.jar 拷贝至 saiku-server/tomcat/webapps/saiku/WEB-INF/lib

2.在 saiku-server 文件夹下执行 `./start-saiku.sh` 命令启动 Saiku，启动后可在浏览器中输入 http://localhost:8080 访问 Saiku 页面。默认端口为 8080，可在 saiku-server/tomcat/conf/server.xml 中进行修改。
![](/images/blog/saiku/start.png)

3.访问 https://licensing.meteorite.bi/login 进行注册并登录。

4.登录后，先创建公司（必须），再创建 License，Hostname 为启动 Saiku 的机器 IP。创建完成后，下载 License 到本地。
![](/images/blog/saiku/license.png)

5.访问 http://localhost:8080/upload.html 上传 License。上传所需用户名与密码均为 admin

6.访问 http://localhost:8080 进行登录，用户名密码均为 admin。如果不执行上述 3-5 步骤，直接登录将会报如下错。

```
Error fetching license. Get a free license from http://licensing.meteorite.bi. You can upload it at /upload.html
```

7.登录后，点击管理控制台，即顶部导航栏最右边包含 A 的图标。然后点击 Add Schema，添加 Mondrian Schema 的 XML 文件和 Schema Name，最后点击 Upload 按钮，显示 Upload Successful! 即可。XML 文件由自己编写，官方提供两个参考样例 Earthquakes.xml 和 FoodMart4.xml，在 saiku-server/data 目录下。具体语法可参考[这个网站](https://mondrian.pentaho.com/documentation/schema.php)
![](/images/blog/saiku/add_schema.png)
如下为本次使用的示例 XML 文件。其中包含一张名为 KYLIN_SALES 的表，包含 TRANS_ID，PART_DT，LSTG_FORMAT_NAME 这 3 个维度和 1 个 SUM(PRICE) 度量。
*注意*：表（列）名大小写敏感，需与 Kylin 中加载的数据源表（列）名称对应

```
<?xml version='1.0'?>
<Schema name='default' metamodelVersion='4.0'>
    <PhysicalSchema>
        <Table name='KYLIN_SALES'>
        </Table>
         </PhysicalSchema>
    <Cube name='kylin_sales_cube' defaultMeasure='Sum PRICE'>
    <Dimensions>
    <Dimension name='Trans_id' table='KYLIN_SALES'>
        <Attributes>
            <Attribute name='Trans_id' keyColumn='TRANS_ID' hasHierarchy='true'/>
        </Attributes>
    </Dimension>
    <Dimension name="Date" table='KYLIN_SALES'>
        <Attributes>
            <Attribute name='Order_date' keyColumn='PART_DT' hasHierarchy='true'/>
        </Attributes>
    </Dimension>
	<Dimension name='Trans_type' table='KYLIN_SALES'>
        <Attributes>
			<Attribute name='Trans_type' keyColumn='LSTG_FORMAT_NAME' hasHierarchy='true'/>
        </Attributes>
    </Dimension>
    </Dimensions>
    <MeasureGroups>
            <MeasureGroup name='SUM' table='KYLIN_SALES'>
                <Measures>
                    <Measure name='Sum PRICE' column='PRICE' aggregator='sum' formatString='Standard'/>
                </Measures>
                  <DimensionLinks>
                    <FactLink dimension='Trans_id'/>
                    <FactLink dimension='Date'/>
					<FactLink dimension='Trans_type'/>
                </DimensionLinks>
            </MeasureGroup>
        </MeasureGroups>
</Cube>
</Schema>
```

8.点击 Add Data Source，填写如下参数。

```
Name: Kylin_Demo
Connection Type: Mondrian
URL: jdbc:kylin://localhost:7070/{project_name}
Schema: {mondrian_schema}
JDBC Driver: org.apache.kylin.jdbc.Driver
Username: {kylin_username} (Default: ADMIN)
Password: {kylin_password} (Default: KYLIN)
Security: (depend on your security setting)
```
点击 Save 按钮保存。

9.点击 Home Tab；点击 Create a new query 进入查询分析页面。
![](/images/blog/saiku/home.png)

10.点击选择多维数据集下拉框，选择刚刚创建好的 Kylin 数据源。
![](/images/blog/saiku/mul_datasets.png)

11.通过拖拽的方式选择维度和度量进行分析。结果以表格形式展示，可在页面右边切换图表模式。
![](/images/blog/saiku/query_result.png)
上图所示拖拽方式会首先转换为 MDX 发送给 Mondrian，Mondrian 会将其转换为 SQL 发送给 Kylin，Kylin 查找可回答该 SQL 语句的 Cube，将查询结果返回给 Mondrian，再由 Mondrian 返回给 Saiku 进行展示。上图所示方式生成的 SQL 语句如下：

```
select "KYLIN_SALES"."TRANS_ID" as "c0", "KYLIN_SALES"."LSTG_FORMAT_NAME" as "c1", sum("KYLIN_SALES"."PRICE") as "m0" from "KYLIN_SALES" as "KYLIN_SALES" group by "KYLIN_SALES"."TRANS_ID", "KYLIN_SALES"."LSTG_FORMAT_NAME"
```


### 常见问题
1. tomcat 启动报错。java.lang.IllegalStateException: Unable to complete the scan for annotations for web application [] due to a StackOverflowError. Possible root causes include a too low setting for -Xss and illegal cyclic inheritance dependencies. The class hierarchy being processed was [org.bouncycastle.asn1.ASN1EncodableVector->org.bouncycastle.asn1.DEREncodableVector->org.bouncycastle.asn1.ASN1EncodableVector]
解决方案：在 saiku-server/tomcat/conf/catalina.properties 中的 tomcat.util.scan.DefaultJarScanner.jarsToSkip=\ 后加,*

2. 选择 schema 后，指标和维度不显示。日志中报如下错：java.lang.NoClassDefFoundError: mondrian/olap/LevelType
解决方案：解压 saiku-server/tomcat/webapps/saiku/WEB-INF/lib 下的 saiku-query-0.4-SNAPSHOT.jar，删除 mondrain 文件夹后重新打包，并替换原有 jar 包。


### 参考文献
- https://saiku-documentation.readthedocs.io/en/latest/
- https://baike.baidu.com/item/XMLA/8711067
- https://community.hitachivantara.com/docs/DOC-1009853
- https://www.jianshu.com/p/14b57d000fe5
- https://blog.csdn.net/csdn_g_y/article/details/78184747
- https://blog.csdn.net/qq_28725695/article/details/84322821