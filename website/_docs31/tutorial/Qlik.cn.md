---
layout: docs31-cn
title:  Qlik Sense 集成
categories: tutorial
permalink: /cn/docs31/tutorial/Qlik.html
since: v2.2
---

Qlik Sense 是新一代自助式数据可视化工具。它是一款完整的商业分析软件，便于开发人员和分析人员快速构建和部署强大的分析应用。近年来，该工具成为全球增长率最快的 BI 产品。它可以与 Hadoop Database（Hive 和 Impala）集成。现在也可与 Apache Kylin 集成。本文将分步指导您完成 Apache Kylin 与 Qlik Sense 的连接。 

### 安装 Kylin ODBC 驱动程序

有关安装信息，参考页面 [Kylin ODBC 驱动](http://kylin.apache.org/cn/docs/tutorial/odbc.html).

### 安装 Qlik Sense

有关 Olik Sense 的安装说明，请访问 [Qlik Sense Desktop download](https://www.qlik.com/us/try-or-buy/download-qlik-sense).

### 与 Qlik Sense 连接

配置完本地 DSN 并成功安装 Qlik Sense 后，可执行以下步骤来用 Qlik Sense 连接 Apache Kylin：

- 打开 **Qlik Sense Desktop**.


- 输入 Qlik 用户名和密码，接着系统将弹出以下对话框。单击**创建新应用程序**.

![](/images/tutorial/2.1/Qlik/welcome_to_qlik_desktop.png)

- 为新建的应用程序指定名称. 

![](/images/tutorial/2.1/Qlik/create_new_application.png)

- 应用程序视图中有两个选项，选择下方的**脚本编辑器**。

![](/images/tutorial/2.1/Qlik/script_editor.png)

- 此时会显示 **数据加载编辑器**的窗口。单击页面右上方的**创建新连接**并选择**ODBC**。

![Create New Data Connection](/images/tutorial/2.1/Qlik/create_data_connection.png)

- 选择你创建的**DSN**，忽略账户信息，点击**创建**。

![ODBC Connection](/images/tutorial/2.1/Qlik/odbc_connection.png)

### 配置Direct Query连接模式
修改默认的脚本中的"TimeFormat", "DateFormat" and "TimestampFormat" 为

`SET TimeFormat='h:mm:ss';`
`SET DateFormat='YYYY-MM-DD';`
`SET TimestampFormat='YYYY-MM-DD h:mm:ss[.fff]';`

考虑到kylin环境中的Cube的数据量级通常都很大，可达到PB级。我们推荐用户使用Qlik sense的Direct Query连接模式，而不要将数据导入到Qlik sense中。

你可以在脚本的连接中打入`Direct Query`来启用Direct Query连接模式。

下面的截图展现了一个连接了 *Learn_kylin* 项目中的 *kylin_sales_cube* 的Direct Query的脚本。

![Script](/images/tutorial/2.1/Qlik/script_run_result.png) 

Qlik sense会基于你定义的这个脚本在报表中相应的生成SQL查询。

我们推荐用户将Kylin Cube上定义的维度和度量相应的定义到脚本中的维度和度量中。

你也可以使用Native表达式来使用Apache Kylin内置函数，例如：

`NATIVE('extract(month from PART_DT)') ` 

完整的脚本提供在下方以供参考。

请确保将脚本中`LIB CONNECT TO 'kylin';` 部分引用的DSN进行相应的修改。 

```SQL
SET ThousandSep=',';
SET DecimalSep='.';
SET MoneyThousandSep=',';
SET MoneyDecimalSep='.';
SET MoneyFormat='$#,##0.00;-$#,##0.00';
SET TimeFormat='h:mm:ss';
SET DateFormat='YYYY/MM/DD';
SET TimestampFormat='YYYY/MM/DD h:mm:ss[.fff]';
SET FirstWeekDay=6;
SET BrokenWeeks=1;
SET ReferenceDay=0;
SET FirstMonthOfYear=1;
SET CollationLocale='en-US';
SET CreateSearchIndexOnReload=1;
SET MonthNames='Jan;Feb;Mar;Apr;May;Jun;Jul;Aug;Sep;Oct;Nov;Dec';
SET LongMonthNames='January;February;March;April;May;June;July;August;September;October;November;December';
SET DayNames='Mon;Tue;Wed;Thu;Fri;Sat;Sun';
SET LongDayNames='Monday;Tuesday;Wednesday;Thursday;Friday;Saturday;Sunday';

LIB CONNECT TO 'kylin';


DIRECT QUERY
DIMENSION 
  TRANS_ID,
  YEAR_BEG_DT,
  MONTH_BEG_DT,
  WEEK_BEG_DT,
  PART_DT,
  LSTG_FORMAT_NAME,
  OPS_USER_ID,
  OPS_REGION,
  NATIVE('extract(month from PART_DT)') AS PART_MONTH,
   NATIVE('extract(year from PART_DT)') AS PART_YEAR,
  META_CATEG_NAME,
  CATEG_LVL2_NAME,
  CATEG_LVL3_NAME,
  ACCOUNT_BUYER_LEVEL,
  NAME
MEASURE
	ITEM_COUNT,
    PRICE,
    SELLER_ID
FROM KYLIN_SALES 
join KYLIN_CATEGORY_GROUPINGS  
on( SITE_ID=LSTG_SITE_ID 
and KYLIN_SALES.LEAF_CATEG_ID=KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID)
join KYLIN_CAL_DT
on (KYLIN_CAL_DT.CAL_DT=KYLIN_SALES.PART_DT)
join KYLIN_ACCOUNT 
on (KYLIN_ACCOUNT.ACCOUNT_ID=KYLIN_SALES.BUYER_ID)
JOIN KYLIN_COUNTRY
on (KYLIN_COUNTRY.COUNTRY=KYLIN_ACCOUNT.ACCOUNT_COUNTRY)
```

点击窗口右上方的**加载数据**，Qlik sense会根据脚本来生成探测查询以检查脚本的语法。

![Load Data](/images/tutorial/2.1/Qlik/load_data.png)

### 创建报表

点击左上角的**应用程序视图**。

![Open App Overview](/images/tutorial/2.1/Qlik/go_to_app_overview.png)

点击**创建新工作表**。

![Create new sheet](/images/tutorial/2.1/Qlik/create_new_report.png)

选择一个图标类型，将维度和度量根据需要添加到图表上。

![Select the required charts, dimension and measure](/images/tutorial/2.1/Qlik/add_dimension.png)

图表返回了结果，说明连接Apache Kylin成功。

现在你可以使用Qlik sense分析Apache Kylin中的数据了。

![View data in Qlik Sense](/images/tutorial/2.1/Qlik/report.png)

请注意如果你希望你的报表可以击中Cube，你在Qlik sense中定义的度量需要和Cube上定义的一致。比如，为了击中Learn_kylin项目的 *Kylin_sales_cube* 我们在本例中使用`sum(price)`。
