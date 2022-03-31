---
layout: post-blog
title: 如何使用 Excel 查询 Kylin？MDX for Kylin！
date: 2022-03-31 11:00:00
author: Xiaoxiang Yu
categories: cn_blog
---

## Kylin 为什么需要 MDX？

### 多维数据库和业务语义层

多维数据库与关系型数据库的关键区别在于业务表达能力。尽管 SQL 表达能力很强，是数据分析师的基本技能，但如果以 “人人都是分析师” 为目标，SQL 和关系数据库对非技术人员还是太难了。从非技术人员的视角，数据湖和数据仓库就好似一个黑暗的房间，知道其中有很多数据，却因为不懂数据库理论和 SQL，无法看清、理解、和使用这些数据。

如何让数据湖(和数据仓库)对非技术人员也 “**清澈见底**”？这就需要引入一个对非技术人员更加友好的“**关系数据模型** – **多维数据模型**”。如果说关系模型描述了数据的技术形态，那么多维模型则描述了数据的业务形态。在多维数据库中，度量对应了每个人都懂的业务指标，维度则是比较、观察这些业务指标的角度。要与上个月比较 KPI，要在平行事业部之间比较绩效，这些是每个非技术人员都理解的概念。通过将关系模型映射到多维模型，本质是在技术数据之上增强了业务语义，形成业务语义层，帮助非技术人员也能看懂、探索、使用数据。

为了增强 Kylin 作为多维数据库的语义层能力，支持多维查询语言是 Kylin Roadmap 上的重点内容，比如 MDX 和 DAX。通过 MDX 可以将 Kylin 中的数据模型转换为业务友好的语言，赋予数据业务价值，方便对接 **Excel**、Tableau 等 BI 工具进行多维分析。

### 基于 MDX 打造业务指标平台 

使用 MDX 来创建复杂的业务指标，相对 SQL 的一些特点和优势如下：

1. 更好的**支持复杂分析场景**，如半累加、多对多、时间窗口分析等；
2. 广泛支持各种 BI，“Kylin + MDX” 不仅能够通过 SQL 接口暴露为类似于关系型数据库的表，也可以暴露为兼容 XMLA 协议的带有语义信息的数据源，可通过 MDX 语言进行查询，支持对接 **Excel** 等只能通过 XMLA 协议对接数据源的 BI；
3. 基于 Kylin 中的数据模型灵活定义 MDX 语义模型，将底层数据结构转换为业务友好的语言，赋予数据业务价值，使得业务人员在分析时无需关心底层的技术复杂度和实现；通过 MDX 模型向外暴露统一语义，帮助用户**构建统一的业务语义层**。(详细信息可以参考链接 1/6/7)



## MDX 介绍

### 什么是 MDX？

MDX (Multi Dimensional eXpression) 是一种 OLAP 多维数据集的查询语言，最初由 Microsoft 于 1997 年作为 OLEDB for OLAP 规范引入，随后集成在 SSAS 中。目前，MDX 在 OLAP 数据库中被广泛采用。

MDX 在很多方面与结构化查询语言 (SQL) 语法相似，但它不是 SQL 语言的扩展；事实上，MDX 所提供的一些功能也可由 SQL 提供，尽管不是那么有效或直观。如同 SQL 查询一样，MDX 查询可以包括SELECT 子句)、FROM 子句和 WHERE 子句。这些关键字以及其它关键字提供了各种工具，用来从多维数据集析取数据的特定部分。

MDX 查询语法示例如下(详细信息可以参考链接 3)： 

```sql
select <axis_specification>[, <axis_specification>]
from <cube_specification>
where <slicer_specification>
```

### MDX 基本概念

在了解学习 MDX 之前，请至少掌握以下概念。限于相关概念在 Microsoft 的 SSAS 官网有比较详细的介绍，不了解的同学可以通过文章末尾的参考链接学习。(详细信息可以参考链接 2/3/4)

1. 维度(Dimensions)、级别(Levels)、成员(Members)和度量值(Measures)
2. 单元(Cell)、元组(Tuple)和集合(Set)
3. 轴维度(Query Axis)和切片器维度(Slicer Axis)

### MDX 和 SQL 的比较

**查询对象**不同，MDX 的查询对象是**多维数据集**(Cube)，是提前 Join 和聚合好的数据，查询时不需要指定 Join 关系。SQL 查询对象是**关系表**(Table)，是一条条的明细记录，查询时需要指定表之间的 Join 关系。 

**查询结果**不同，SQL 返回**二维数据子集**，而 MDX 返回**多维数据集**。(详细信息可以参考链接 5)

# MDX for Kylin 介绍

### 什么是 MDX for Kylin ?

**MDX for Kylin** 是基于 **Mondrian** 二次开发的，由 **Kyligence** 贡献的，使用 **Apache Kylin 4** 作为数据源的 MDX 查询引擎 。MDX for Kylin 的使用体验比较接近 Microsoft SSAS，可以集成多种数据分析工具，包括 Microsoft Excel、Tableau 等，可以为大数据分析场景下提供更极致的体验。

### MDX for Kylin 创建业务指标

#### 原子指标和业务指标

在 Kylin Cube 我们创建的各种度量，是在单独的一列上的进行的聚合计算(TopN 除外)，只包含了有限的几种聚合函数，即 Sum/Max/Min/Count/Count Distinct，相对比较简单，我们称之为**原子指标**。

在实际业务场景中，基于原子指标我们可以对**原子指标**的各种复杂运算，来创建有业务含义的复合指标，这样的指标我们称之为**业务指标。**

#### 层级结构、计算度量和命名集

**层级结构：**层级结构是基于维度的级别集合，可用于提高数据分析人员的分析能力。例如，你可以创建一个时间层级结构，包含了年、季、月、周和日级别。这样分析人员在客户端中可以先逐年分析销售额，在需要时可以分别展开“季度 > 月 > 周 > 日”来进行更细粒度的分析。

**计算度量：**计算度量是对**原子指标**使用 MDX 表达式进行复合计算形成的新的度量/指标，我们主要使用计算度量来创建**业务指标**。

**命名集：**在 MDX for Kylin 的使用中，经常会出现需要重复使用一组成员的需求，这种需求可以通过定义命名集来满足。命名集(NamedSet) 是根据指定的表达式 计算得到的一个成员Set，即一组成员的集合，命名集可以直接置于轴上用于展示，也可以在计算度量或其他命名集的表达式中使用。

#### 创建语义模型

在 Kylin 4 根据表与表之间的关系创建数据模型，并且在 Cube 上定义维度和度量，这些度量我们可以认为是**原子指标**。

在 MDX for Kylin，将相关联的 Kylin Cube 相关联，来创建数据集；并且基于原子指标，来创建有业务含义的**业务指标。**

![](/images/blog/how_to_use_excel_to_query_kylin/1_use_excel_to_query_kylin.cn.png)

#### 数据分析

使用时，客户端发送 MDX 查询给 MDX for Kylin，MDX for Kylin 再解析 MDX 查询翻译为 SQL 并且发送给 Kylin ，然后 Kylin 通过预计算的 Cuboid 回答 SQL 查询并把结果交还给 MDX for Kylin，MDX for Kylin 会再做一些衍生指标的计算，最终将多维数据结果返回给客户端。

![](/images/blog/how_to_use_excel_to_query_kylin/2_use_excel_to_query_kylin.cn.png)

#### 流程总结

总的来说，支持 MDX 接口能够增强 Kylin 的语义层能力，为用户带来统一的数据分析和管理体验，更好地发挥数据的价值。下图就是将从下而上，展示从原始数据加工业务指标的过程。

![](/images/blog/how_to_use_excel_to_query_kylin/3_use_excel_to_query_kylin.cn.png)

### MDX for Kylin 的技术优势

MDX for Kylin 相对其它开源 MDX 查询引擎，具有以下优势：

- 更好支持BI(Excel/Tableau/Power BI等) 产品，适配 XMLA 协议；
- 针对 BI 的 MDX Query 进行了特定优化重写；
- 适配 Kylin 查询，通过 Kylin 的预计算能力加速 MDX 查询；
- 通过简洁易懂的操作界面，提供了统一的指标定义和管理能力。

# 从 Docker 快速开始

### 测试环境

- Macbook Pro 笔记本
  - Docker Desktop (latest version) 
- Windows 10 虚拟机
  - Microsoft Excel (for Windows)

### 启动容器

这个容器包含了 Yarn、HDFS、MySQL、Kylin、MDX for Kylin 等进程。

```she
docker run -d \
    -m 8g \
    -p 7070:7070 \
    -p 7080:7080 \
    -p 8088:8088 \
    -p 50070:50070 \
    -p 8032:8032 \
    -p 8042:8042 \
    -p 2181:2181 \
    --name kylin-4.0.1 \
    apachekylin/apache-kylin-standalone:kylin-4.0.1-mondrian
```

### 检查环境

等待一段时间，请依次检查 HDFS/YARN/Kylin/MDX for Kylin 的 Web UI 是否可以访问。

| **组件**      | **Web UI 地址**                       |
| ------------- | ------------------------------------- |
| HDFS          | http://localhost:50070/dfshealth.html |
| YARN          | http://localhost:8088/cluster         |
| Kylin         | http://localhost:7070/kylin           |
| MDX for Kylin | http://localhost:7080/overview        |

### 构建样例 Cube

请直接使用 Kylin 自带的样例 Cube：`kylin_sales_cube`。

![](/images/blog/how_to_use_excel_to_query_kylin/4_use_excel_to_query_kylin.cn.png)

### 创建 MDX 数据集

##### 登录 MDX for Kylin

默认账号/密码是 ADMIN/KYLIN，MDX for Kylin 的账户与 KYLIN 的同步。

![](/images/blog/how_to_use_excel_to_query_kylin/5_use_excel_to_query_kylin.cn.png)

##### 创建数据集和定义关系

![](/images/blog/how_to_use_excel_to_query_kylin/6_use_excel_to_query_kylin.cn.png)

![](/images/blog/how_to_use_excel_to_query_kylin/7_use_excel_to_query_kylin.cn.png)

##### 创建时间层级

- 修改`KYLIN_CAL_DT`的表属性

![](/images/blog/how_to_use_excel_to_query_kylin/8_use_excel_to_query_kylin.cn.png)

- 修改 `YEAR_BEG_DT` 的类型为“年”

![](/images/blog/how_to_use_excel_to_query_kylin/9_use_excel_to_query_kylin.cn.png)

![](/images/blog/how_to_use_excel_to_query_kylin/10_use_excel_to_query_kylin.cn.png)

- 同理修改`MONTH_BEG_DT`和`WEEK_BEG_DT` ，并且选择对应的层级
- 创建时间层级`Calendar` ，请注意设置层级结构的前后顺序保持为“年-月-周”

![](/images/blog/how_to_use_excel_to_query_kylin/11_use_excel_to_query_kylin.cn.png)

##### 修改原子指标名称

- 修改 `GMV_SUM` 为 “销售额”，修改`SELLER_CNT_HLL`为“商家数量”

![](/images/blog/how_to_use_excel_to_query_kylin/12_use_excel_to_query_kylin.cn.png)

##### 创建业务指标(计算度量)

- 创建业务指标“商户平均消费额”

![](/images/blog/how_to_use_excel_to_query_kylin/13_use_excel_to_query_kylin.cn.png)

- 依次创建业务指标“销售额年同期增长率”和“销售额月同期增长率”

| **指标名称**       | **MDX 表达式**                                               |
| ------------------ | ------------------------------------------------------------ |
| 商户平均消费额     | [Measures].[销售额]/[Measures].[商家数量]                    |
| 销售额年同期增长率 | [Measures].[销售额] / SUM(  ParallelPeriod(    [KYLIN_CAL_DT].[Calendar-Hierarchy].[YEAR_BEG_DT],    1,    [KYLIN_CAL_DT].[Calendar-Hierarchy].CurrentMember  ),  [Measures].[销售额] ) - 1 |
| 销售额月同期增长率 | [Measures].[销售额] / SUM(  ParallelPeriod(    [KYLIN_CAL_DT].[Calendar-Hierarchy].[MONTH_BEG_DT],    1,    [KYLIN_CAL_DT].[Calendar-Hierarchy].CurrentMember  ),  [Measures].[销售额] ) - 1 |
| 总销售额           | Fixed([KYLIN_CAL_DT].[YEAR_BEG_DT], [Measures].[销售额])     |
| 全年销售额占比     | [Measures].[销售额]/[Measures].[总销售额]                    |



### HTTP API 测试 MDX 查询

如果你没有 一个 Windows 环境的 Excel，并且你想测试你在上一步测试创建的业务指标，请通过  MDX 暴露的查询相关的 REST API 来验证查询结果。如果想修改 MDX 查询语句，请修改以下`<Statement></Statement>` 里面的 MDX 语句，并且请根据情况修改`Catalog`字段的值。

```she
curl --location --request POST 'http://localhost:7080/mdx/xmla/learn_kylin' \
--header 'Authorization: Basic QURNSU46S1lMSU4=' \
--header 'Connection:  Keep-Alive' \
--header 'SOAPAction: "urn:schemas-microsoft-com:xml-analysis:Execute"' \
--header 'User-Agent: MSOLAP' \
--header 'Content-Type: text/xml' \
--header 'Accept: */*' \
--header 'Cookie: JSESSIONID=22BF2B6D889F183D7F7E898D4D769398; MDXAUTH=ZUt6V1VBRE1JTjoyYTk3Zjg2NTdiNjk0NTE5NzA0NjFiN2ZjYTNkYzg2OToxNjQ2NjMxNDkw' \
--data-raw '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Header>
        <Session xmlns="urn:schemas-microsoft-com:xml-analysis" SessionId="8nblet191q"/>
    </soap:Header>
    <soap:Body>
        <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
           <Command>
                <Statement>
SELECT {[Measures].[销售额],
        [Measures].[商家数量],
        [Measures].[商户平均消费额],
        [Measures].[全年销售额占比],
        [Measures].[销售额年同期增长率],
        [Measures].[销售额月同期增长率]} 
    DIMENSION PROPERTIES PARENT_UNIQUE_NAME ON COLUMNS , 
NON EMPTY Hierarchize(AddCalculatedMembers({DrilldownLevel({[KYLIN_CAL_DT].[Calendar-Hierarchy].[All]})})) 
    DIMENSION PROPERTIES PARENT_UNIQUE_NAME ON ROWS  
FROM [demo0] 
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
                </Statement>            
            </Command>
            <Properties>
                <PropertyList>
                    <Catalog>demo0</Catalog>
                    <Timeout>0</Timeout>
                    <Content>SchemaData</Content>
                    <Format>Multidimensional</Format>
                    <AxisFormat>TupleFormat</AxisFormat>
                    <DbpropMsmdFlattened2>false</DbpropMsmdFlattened2>
                    <SafetyOptions>2</SafetyOptions>
                    <Dialect>MDX</Dialect>
                    <MdxMissingMemberMode>Error</MdxMissingMemberMode>
                    <DbpropMsmdOptimizeResponse>9</DbpropMsmdOptimizeResponse>
                    <DbpropMsmdActivityID>6C94075F-65AD-4B9E-B3EB-4536A191A6AB</DbpropMsmdActivityID>
                    <DbpropMsmdRequestID>9FA20B8A-ACA0-414E-98EA-14649F20CF75</DbpropMsmdRequestID>
                    <LocaleIdentifier>1033</LocaleIdentifier>
                    <DbpropMsmdMDXCompatibility>1</DbpropMsmdMDXCompatibility>
                </PropertyList>
            </Properties>
        </Execute>
    </soap:Body>
</soap:Envelope>'
```



### 通过 Excel 透视表访问业务指标

##### 连接 MDX for Kylin

- 打开 Microsoft Excel (for Windows)

![](/images/blog/how_to_use_excel_to_query_kylin/14_use_excel_to_query_kylin.cn.png)

- 配置 MDX for Kylin 地址，请替换 IP_Adress 为你笔记本的 IP 地址，用户名和密码使用 Kylin 的账号和密码。

![](/images/blog/how_to_use_excel_to_query_kylin/15_use_excel_to_query_kylin.cn.png)

##### 通过数据透视表分析销售额

- 配置数据透视表

![](/images/blog/how_to_use_excel_to_query_kylin/16_use_excel_to_query_kylin.cn.png)

- 查看年同期销售额增长率

![](/images/blog/how_to_use_excel_to_query_kylin/17_use_excel_to_query_kylin.cn.png)

- 查看月同期销售额增长率

![](/images/blog/how_to_use_excel_to_query_kylin/18_use_excel_to_query_kylin.cn.png)



## 参考链接

| **编号** | **链接**                                                     | **注释**                        | **产品**       |
| -------- | ------------------------------------------------------------ | ------------------------------- | -------------- |
| 1        | https://lists.apache.org/thread/4fkhyw1fyf0jg5cb18v7vxyqbn6vm3zv | Kylin 社区发起开发语义层的讨论  | Apache Kylin   |
| 2        | https://mondrian.pentaho.com/documentation/mdx.php           | Mondrian 的官网文档             | Mondrian       |
| 3        | https://docs.microsoft.com/en-us/sql/mdx/mdx-syntax-elements-mdx | SSAS 关于 MDX 查询的语法规范    | Microsoft SSAS |
| 4        | https://wiki.smartbi.com.cn/pages/viewpage.action?pageId=76692713 | SmartBI 关于 MDX 基本概念的介绍 | SmartBI        |
| 5        | https://dba.stackexchange.com/questions/138311/good-example-of-mdx-vs-sql-for-analytical-queries | 对比 SQL 和 MDX                 | N/A            |
| 6        | https://kyligence.io/blog/opportunities-for-ssas-in-the-cloud/ | Kyligence MDX 技术博客          | Kyligence      |
| 7        | https://kyligence.io/blog/semantic-layer-the-bi-trend-you-dont-want-to-miss-in-2020/ | Kyligence MDX 技术博客          | Kyligence      |
| 8        | https://docs.kyligence.io/books/mdx/v1.3/zh-cn/index.html    | Kyligence MDX 用户手册          | Kyligence      |
| 9        | https://medium.com/airbnb-engineering/how-airbnb-achieved-metric-consistency-at-scale-f23cc53dea70 | Airbnb Tech Blog                | Airbnb Minerva |

