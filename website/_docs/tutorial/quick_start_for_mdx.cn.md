---
layout: docs-cn
title: 快速上手 MDX for Kylin 数据集分析
categories: tutorial
permalink: /cn/docs/tutorial/quick_start_for_mdx.html
---

欢迎查看本教程。 本教程通过数据集 `Kylin Sales`，构建一个数据集 ，并说明如何创建并在 Excel 中使用数据集进行分析。章节快速导航：

- [您将掌握的内容](#您将掌握的内容)
- [前置条件](#前置条件)
- [准备操作](#准备操作)
- [创建 MDX 数据集](#创建 MDX 数据集)
  - [填写基本信息](#填写基本信息)
  - [定义模型关系](#定义模型关系)
  - [修改维度名称和度量名称](#修改维度名称和度量名称)
    - [修改维度名称](#修改维度名称)
    - [修改度量名称](#修改度量名称)
  - [修改维度表属性和维度属性](#修改维度表属性和维度属性)
  - [创建层级结构](#创建层级结构)
  - [创建计算度量](#创建计算度量)
  - [定义翻译](#定义翻译)
  - [编辑维度用法](#编辑维度用法)
  - [确定并保存](#确定并保存)
- [在 Excel 中分析](#在 Excel 中分析)

## 您将掌握的内容<a id="您将掌握的内容"></a>

在本教程中，您将了解以下内容：

- 如何在 MDX for Kylin 中创建数据集；
- 如何在 MDX for Kylin 的数据集中创建层级结构和常用的计算度量（例如 YTD、QTD、MTD、YOY 和 MOM）；
- 如何在 MDX for Kylin 的数据集中修改维度表、维度属性、度量属性和维度用法。
- 如何通过 Excel 连接 MDX for Kylin 进行分析。

## 前置条件<a id="前置条件"></a>

要完成本教程的所有课程，您将需要准备如下内容：

- 本次教程主要以 MDX for Kylin Docker 环境作为演示。
- 完成 MDX for Kylin Docker 的安装，详见 [快速试用 MDX for Kylin](/cn/docs/install/kylin_docker.html)。
- 在 MDX for Kylin 对接的 Kylin 中构建 Kylin Sales 数据集。

## 准备操作<a id="准备操作"></a>

1. 在 Kylin 中创建 Kylin Sales 的 Model/Cube。

   > 当前 Docker 环境启动默认带有 `learn_kylin`项目，并且生成 `kylin_sales_model` 和 `kylin_sales_cube`.

   ![kylin sales](/images/tutorial/4.0/Quick-Start-For-Mdx/kylin_sales.png)

2. 构建 `kylin_sales_cube`。

   ![built cube](/images/tutorial/4.0/Quick-Start-For-Mdx/kylin_sales_ready_cube.png)

## 创建 MDX 数据集<a id="创建 MDX 数据集"></a>

至此，我们已经完成了准备工作，接下来就可以开始进行分析工作。

### 填写基本信息<a id="填写基本信息"></a>

1. 由于 Kylin 中的 Cube 创建在项目 `learn_kylin` 中，所以此处项目名称选择 `learn_kylin`。
   ![mdx project](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_project_cn.png)
2. 打开**数据集**菜单，点击**创建数据集**按钮。
   ![mdx dataset](/images/tutorial/4.0/Quick-Start-For-Mdx/create_mdx_dataset_cn.png)
3. 然后您将进入数据集设计页面。填写数据集名称： **Kylin_Sales_Analysis** 。完成后点击 **下一步** 按钮。
   ![mdx dataset step 01](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_dataset_step01_cn.png)

### 定义模型关系<a id="定义模型关系"></a>

此次分析将简单分析关于 Kylin_Sales 的数据是否存在某种关系。

拖拽模型 `kylin_sales_cube` 进入右侧画布, 完成后点击 **下一步** 按钮。
![mdx dataset step 02](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_dataset_step02_cn.png)

### 修改维度名称和度量名称<a id="修改维度名称和度量名称"></a>

为了增加可读性，我们可以更改维度和度量的名称。

#### 修改维度名称<a id="修改维度名称"></a>

1. 点击定义语义页面左侧的**维度名称**，即可显示维度详情。
   ![modify dimession](/images/tutorial/4.0/Quick-Start-For-Mdx/modified_dimession_cn.png)

2. 在显示维度详情页面，点击**编辑**按钮，即可进入编辑页面，此处将维度 **YEAR_BEG_DT** 的名称改为 **年份**。
   ![modify dimession 02](/images/tutorial/4.0/Quick-Start-For-Mdx/modified_dimession02.png)

同样的依照如上方式，并按下表，更改维度名称：

| Cube 名称        | 表名称       | 原维度名称   | 更改后维度名称 |
| ---------------- | ------------ | ------------ | -------------- |
| kylin_sales_cube | KYLIN_CAL_DT | YEAR_BEG_DT  | 年份           |
| kylin_sales_cube | KYLIN_CAL_DT | MONTH_BEG_DT | 月份           |
| kylin_sales_cube | KYLIN_CAL_DT | WEEK_BEG_DT  | 周             |

#### 修改度量名称<a id="修改度量名称"></a>

1. 同样的，点击定义语义页面左侧的**度量名称**，即可显示度量详情。
   ![modify measure](/images/tutorial/4.0/Quick-Start-For-Mdx/modified_measure_cn.png)

2. 在显示度量详情页面，点击**编辑**按钮，即可进入编辑页面，此处将度量 **GMV_SUM** 的名称改为 **销售总额**。
   ![modify measure 02](/images/tutorial/4.0/Quick-Start-For-Mdx/modified_measure02_cn.png)

同样的依照如上方式，并按下表，更改度量名称：

| Cube 名称        | 原度量名称     | 更改后度量名称 |
| ---------------- | -------------- | -------------- |
| kylin_sales_cube | TRANS_CNT      | 销售订单总数   |
| kylin_sales_cube | SELLER_CNT_HLL | 销售用户总数   |
| kylin_sales_cube | ITEM_BITMAP    | 销售用品总数   |
| kylin_sales_cube | GMV_SUM        | 销售总额       |

### 修改维度表属性和维度属性<a id="修改维度表属性和维度属性"></a>

为了计算 YTD、MTD、QTD 等时间智能函数，所以需要调整相关维度表和维度的属性。我们需要将维度表属性改为时间类型。并将年、季、月的维度属性分别更改成年、季、月。

1. **修改维度表属性**
   点击定义语义页面左侧的**维度表名称**，即可显示维度表详情。
   
   ![dimession property](/images/tutorial/4.0/Quick-Start-For-Mdx/dimension_property_cn.png)
   
   在显示度量详情页面，点击**编辑**按钮，即可进入编辑页面，此处将维度表属性由常规，改为时间。
   
   ![dimession proper](/images/tutorial/4.0/Quick-Start-For-Mdx/dimension_property02_cn.png)

2. **修改维度属性**
   按照上文介绍的方法，进入编辑维度属性页面，即可更改其属性，此处需要将维度年份、季度和月份的属性改为年、季和月。
   
   ![dimension property 03](/images/tutorial/4.0/Quick-Start-For-Mdx/dimension_property03_cn.png)

### 创建层级结构<a id="创建层级结构"></a>

对于时间维度，例如年、月、日这样的具有层级关系结构的维度，我们一般会建立层级结构，用于分析

> 提示：
> 
> 若需要使用YTD、QTD、MTD、WTD这类时间智能函数，有以下注意事项：
>
> 1. 必须要建立日历层级结构
> 2. 层级结构中的维度的属性分别被指定为年、季、月、周、日
> 3. 在 Excel 使用时间智能函数时，需要将该层级结构拖入行或者列中

点击 维度列表的右上角的  **新增层级结构** 按钮，以新增层级结构。此处由于分析的需要，创建名为日历结构的层级结构，其中的元素为年份、季度、月份、周、日期。

> 提示：
> 
> 层级结构中的维度，需要按照概念从大到小的顺序选择，例如，年、月、周、日。

![add hierachy](/images/tutorial/4.0/Quick-Start-For-Mdx/add_hierachy_cn.png)

### 创建计算度量<a id="创建计算度量"></a>

点击度量列上右上角的 **新增计算度量** 按钮即可添加计算度量。

![add calculate measure](/images/tutorial/4.0/Quick-Start-For-Mdx/calculate_measure_cn.png)

下方表格列出了常见的计算度量表达式，供用户参考。

| 计算度量名称       | **格式** | MDX 表达式                                                   |
| ------------------ | -------- | ------------------------------------------------------------ |
| 销售产品数量 YTD   | #,###,00 | SUM(YTD([DATE_DIM].[日历结构-Hierarchy].CurrentMember),[Measures].[销售产品数量]) |
| 销售产品数量 QTD   | #,###,00 | SUM(QTD([DATE_DIM].[日历结构-Hierarchy].CurrentMember),[Measures].[销售产品数量]) |
| 销售产品数量 MTD   | #,###,00 | SUM(MTD([DATE_DIM].[日历结构-Hierarchy].CurrentMember),[Measures].[销售产品数量]) |
| 上月销售产品数量   | #,###,00 | SUM(ParallelPeriod([DATE_DIM].[日历结构-Hierarchy].[月份],1,[DATE_DIM].[日历结构-Hierarchy].CurrentMember),[Measures].[销售产品数量]) |
| 上季度销售产品数量 | #,###,00 | SUM(ParallelPeriod([DATE_DIM].[日历结构-Hierarchy].[季度],1,[DATE_DIM].[日历结构-Hierarchy].CurrentMember),[Measures].[销售产品数量]) |
| 去年销售产品数量   | #,###,00 | SUM(ParallelPeriod([DATE_DIM].[日历结构-Hierarchy].[年份],1,[DATE_DIM].[日历结构-Hierarchy].CurrentMember),[Measures].[销售产品数量]) |
| 销售产品数量 MOM   | 0.00%    | ([Measures].[销售产品数量]-[Measures].[上月销售产品数量])/[Measures].[上月销售产品数量] |
| 销售产品数量 YOY   | 0.00%    | ([Measures].[销售产品数量]-[Measures].[上月销售产品数量])/[Measures].[上年销售产品数量] |

### 定义翻译<a id="定义翻译"></a>

在 `MDX for Kylin` 中，您可以嵌入一个基于区域设置标识符 （LCID）来设置指定地域的信息来获取对应区域的语言设置，并展示对应数据集各项实体的名称

这项功能有助于国际化的团队分析同一份数据集，从而节省重复定义数据集的工作，同时也保证不同的团队使用的数据口径是统一的。

#### 备注

1. 目前仅支持中国、美国、英国三种语言设置。
2. 目前可设置的实体为维表、维度、度量、计算度量和层级。如果某维度定义了翻译，那么包含该维度的层级也会使用翻译的名称。
3. 如果分析应用客户端使用的语言没有在数据集定义，那么数据集将采用默认语言。如果您使用是 Excel，不同语言设置的 Excel 将会使用不同的 LCID。
4. 在 MDX 表达式中引用维度，度量等实体时，请使用默认名称进行引用，而不要使用翻译的名称。

#### 配置步骤<a id="配置步骤"></a>

1. 点击添加翻译语言的按钮；
   ![mdx translation](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_translation_cn.png)

2. 在增加的一列中选择翻译对应的语言，并点击右侧的勾确定添加；
   ![mdx translation 02](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_translation02_cn.png)

3. 在新增的翻译语言中根据需求，可以设置对应维表、维度、度量名称等的翻译。
   ![mdx translation 03](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_translation03_cn.png)

> 以上翻译定义了`年月`的中英文翻译。

### 编辑维度用法<a id="编辑维度用法"></a>

由于 Kylin 暂不支持复杂的多对多关系等，所以此处不做特殊定义。

### 确定并保存<a id="确定并保存"></a>

至此您已经完成了本教程中数据集创建的主要工作，点击 **确定** 按钮即可。

![mdx save](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_save_cn.png)

## 在 Excel 中分析<a id="在 Excel 中分析"></a>

我们已经完成了数据集的定义，现在就可以对接 Excel 进行分析了。

1. 选择 **数据 —> 自 Analysis Services**
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_cn.png" alt="mdx excel" style="zoom:50%;" />

2. 接下来您需要在**服务器名称**一栏中填写连接 Kyligence MDX 的地址信息，样例如下：
   ``` http://{host}:{port}/mdx/xmla/{project}```
   MDX for Kylin 默认端口号是 7080，用户名和密码请填写 MDX for Kylin 系统的登录用户名和密码。
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_02_cn.png" alt="mdx excel 02" style="zoom:50%;" />

3. 现在Excel 已经被连接到 数据集了。选择此前创建的的数据集 **Kylin_Sales_Analysis**，点击下一步。
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_03_cn.png" alt="mdx excel 03" style="zoom:50%;" />

4. 勾选总是尝试文件来刷新数据，点击完成。
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_04_cn.png" alt="mdx excel 04" style="zoom:50%;" />

5. 现在，您可以使用 Excel 透视表分析 MDX for Kylin 的数据集了。
   ![mdx excel 05](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_05_cn.png)
