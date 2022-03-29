---
layout: docs
title:  Quickly Start With MDX for Kylin
categories: tutorial
permalink: /docs/tutorial/quick_start_for_mdx.html
---

Welcome to this tutorial. In this tutorial, we will build an MDX dataset base on the public dataset `KYLIN_SALES`, showing how to create and use datasets for analysis in Excel. Chapter Quick Navigation:

- [What you will know](#What you will know)
- [Prerequisites](#Prerequisites)
- [Preparation](#Preparation)
- [Create MDX Dataset](#Create MDX Dataset)
  - [Fill in the basic information](#Fill in the basic information)
  - [Define Relationships](#Define Relationships)
  - [Modify dimension name and measure name](#Modify dimension name and measure name)
    - [Modify the dimension name](#[Modify the dimension name](#))
    - [Modify the measure name](#Modify the measure name)
  - [Modify dimension table attributes and dimension attributes](#Modify dimension table attributes and dimension attributes)
    - [Modify dimension table properties](#Modify dimension table properties)
    - [Modify dimension attributes](#Modify dimension attributes)
  - [Create Hierarchies](#Create Hierarchies)
  - [Create Calculated Measures](#Create Calculated Measures)
  - [Define Translation](#Define Translation)
  - [Edit Dimension Usage](#Edit Dimension Usage)
  - [Edit Visibility Restrict](#Edit Visibility Restrict)
  - [Save and Create](#Save and Create)
- [Analysis in Excel](#Analysis in Excel)
- [Manual & Source Code](#Manual & Source Code)


## What you will know<a id="What you will know"></a>

In this tutorial you will learn about the following:

- How to create MDX datasets in `MDX for Kylin`.
- How to create hierarchies and commonly used calculated measures (eg YTD, QTD, MTD, YOY and MOM) in `MDX for Kylin`'s dataset.
- How to modify dimension table attributes, dimension attributes, measure attributes, and dimension usage in `MDX for Kylin`'s dataset.
- How to analysis in Excel.

## Prerequisites<a id="Prerequisites"></a>

To complete all the lessons in this tutorial, you will need to prepare the following:

- This tutorial mainly uses Docker of MDX for Kylin as a demonstration.
- Complete the installation of Docker of MDX for Kylin, as described in [Quickly try MDX for Kylin](/docs/install/kylin_docker.html).
- Prepare `Kylin Sales` cube which status must be ready in Kylin.

## Preparation<a id="Preparation"></a>

1. **Create Models/Cubes in Kylin**

   > The current docker environment starts with the `learn_kylin` project by default, and generates `kylin_sales_model` and `kylin_sales_cube`.

   ![kylin sales](/images/tutorial/4.0/Quick-Start-For-Mdx/kylin_sales.png)

2. Build `kylin_sales_cube`.
   ![imag](/images/tutorial/4.0/Quick-Start-For-Mdx/kylin_sales_ready_cube.png)

## Create MDX Dataset<a id="Create MDX Dataset"></a>

At this point, we have completed the preparation work, and then we should start creating MDX dataset.

### Fill in the basic information<a id="Fill in the basic information"></a>

1. Since the Cube in Kylin is created in the project `learn_kylin`, the project name in MDX for Kylin should be `learn_kylin`.
   ![mdx project](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_project.png)

2. Open the **Dataset** menu and click the **+ Create Dataset** button.
   ![create dataset](/images/tutorial/4.0/Quick-Start-For-Mdx/create_mdx_dataset.png)

3. Click to start creating a dataset. 
   Fill in the dataset Name with ***Kylin_Sales_Analysis\***. 
   Click **Next** button when done.
   ![create dataset](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_dataset_step01.png)

### Define Relationships<a id="Define Relationships"></a>

This analysis will simply analyze whether there is a relationship between the data about `Kylin_Sales`.

Drag `kylin_sales_cube` to the right pane. Click the **Next** button when done.

![define relationships](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_dataset_step02.png)

### Modify dimension name and measure name<a id="Modify dimension name and measure name"></a>

To increase readability, we can change the names of dimensions and measures.

#### Modify the dimension name<a id="Modify the dimension name"></a>

1. Click on the **Dimension Name** on the left side of the Define Semantics page to display the dimension details.
   ![modify dimession](/images/tutorial/4.0/Quick-Start-For-Mdx/modified_dimession.png)
2. On the Display Dimension Details page, click the **Edit** button to go to the edit page to change dimension name from **YEAR_BEG_DT** to **YEAR**.
   ![modify dimession](/images/tutorial/4.0/Quick-Start-For-Mdx/modified_dimession02.png)

3. On the same way, we could change the dimension name by the following table:

| Cube Name        | Table Name   | Original Name | Changed Name |
| ---------------- | ------------ | ------------- | ------------ |
| kylin_sales_cube | KYLIN_CAL_DT | YEAR_BEG_DT   | Year         |
| kylin_sales_cube | KYLIN_CAL_DT | MONTH_BEG_DT  | Month        |
| kylin_sales_cube | KYLIN_CAL_DT | WEEK_BEG_DT   | Week         |

#### Modify the measure name<a id="Modify the measure name"></a>

1. Similarly, click on the **measure name** on the left side of the definition semantics page to display the metric details.
   ![modify measure](/images/tutorial/4.0/Quick-Start-For-Mdx/modified_measure.png)

2. On the Display Metrics Details page, click the **Edit** button to go to the Edit page to change dimension name from **GMV_SUM** to **Total_Sales_Amount**.
   ![modified measure](/images/tutorial/4.0/Quick-Start-For-Mdx/modified_measure02.png)
3. On the same way, we could change the measure name according to the table below:

| Cube Name        | Original Name  | Changed Name       |
| ---------------- | -------------- | ------------------ |
| kylin_sales_cube | TRANS_CNT      | Total_Orders_Count |
| kylin_sales_cube | SELLER_CNT_HLL | Sellers_Count      |
| kylin_sales_cube | ITEM_BITMAP    | Items_Count        |
| kylin_sales_cube | GMV_SUM        | Total_Sales_Amount |

### Modify dimension table attributes and dimension attributes<a id="Modify dimension table attributes and dimension attributes"></a>

In order to calculate time intelligence functions such as YTD, MTD, and QTD, it is necessary to adjust the attributes of the related dimension tables and dimensions. We need to change the dimension table property to a time type, and the dimension attributes of year, season and month to year, season and month respectively.

1. **Modify dimension table properties**

   Click on the **Dimension Table Name** on the left side of the Define Semantic page to display the dimension table details.
   ![dimension property](/images/tutorial/4.0/Quick-Start-For-Mdx/dimension_property.png)

   On the `Show Dimension Table Properties` page, click the **Edit** button to go to the edit page, where the dimension table properties are changed from regular to time.

   ![dimenssion property](/images/tutorial/4.0/Quick-Start-For-Mdx/dimension_property02.png)

2. **Modify dimension attributes**

   According to the method described above, you can change the properties of the edit dimension property page. Here you need to change the properties of the dimension year, quarter and month to year, season and month, and week.

![modify dimenssion](/images/tutorial/4.0/Quick-Start-For-Mdx/dimension_property03.png)

### Create Hierarchies<a id="Create Hierarchies"></a>

For time dimensions, such as the year, month, day, and other dimensions with hierarchical structure, we generally establish a hierarchy for analysis.

> Notes：
>   
> If you need to use time intelligence functions such as YTD, QTD, MTD, WTD, there are the following precautions:
>
> 1. Calendar hierarchy must be established.
> 2. The attributes of the dimensions in the hierarchy are specified as year, season, month, week, and day respectively.
> 3. When using time intelligence functions in Excel, you need to drag the hierarchy into rows or columns.

Click the **Add Hierrachy** button to add a hierarchy. Here, due to the needs of the analysis, create a hierarchiy called the calendar with dimensions of year, quarter, month, and date.

> Note:
> 
> Dimensions in the hierarchy need to be selected in order of concept, for example, year, month, week, and then day.

![add hierachy](/images/tutorial/4.0/Quick-Start-For-Mdx/calculate_measure.png)

### Create Calculated Measures<a id="Create Calculated Measures"></a>

Add a calculated measure by clicking the **Add Calculated Measure** button.

![add calculate](/images/tutorial/4.0/Quick-Start-For-Mdx/calculate_measure.png)

The table below lists common expressions, which you could refer to in your projects.

| Calculated Measures | Format String | MDX Expression                                               |
| ------------------- | ------------- | ------------------------------------------------------------ |
| Sales_YTD           | #,###,00      | SUM(YTD([DATE_DIM].[calendar-Hierarchy].CurrentMember),[Measures].[Sales_Qty]) |
| Sales_QTD           | #,###,00      | SUM(QTD([DATE_DIM].[calendar-Hierarchy].CurrentMember),[Measures].[Sales_Qty]) |
| Sales_MTD           | #,###,00      | SUM(MTD([DATE_DIM].[calendar-Hierarchy].CurrentMember),[Measures].[Sales_Qty]) |
| Sales_PM            | #,###,00      | SUM(ParallelPeriod([DATE_DIM].[calendar-Hierarchy].[Month],1,[DATE_DIM].[calendar-Hierarchy].CurrentMember),[Measures].[Sales_Qty]) |
| Sales_PQ            | #,###,00      | SUM(ParallelPeriod([DATE_DIM].[calendar-Hierarchy].[Quarter],1,[DATE_DIM].[calendar-Hierarchy].CurrentMember),[Measures].[Sales_Qty]) |
| Sales_PY            | #,###,00      | SUM(ParallelPeriod([DATE_DIM].[calendar-Hierarchy].[Year],1,[DATE_DIM].[calendar-Hierarchy].CurrentMember),[Measures].[Sales_Qty]) |
| Sales_MOM           | 0.00%         | ([Measures].[Sales_Qty]-[Measures].[Sales_PM])/[Measures].[Sales_PM] |
| Sales_YOY           | 0.00%         | ([Measures].[Sales_Qty]-[Measures].[Sales_PY])/[Measures].[Sales_PY] |

### Define Translation<a id="Define Translation"></a>

In MDX for Kylin, you can embed multiple translation of a caption to provide culture-specific strings based on the localed identifier (LCID).

This feature helps international teams to consume the same dataset without the need to repeatedly defining the dataset for localization purposes.

#### Note<a id="Note"></a>

1. There are three language supported: Chinese, English(US), and English(UK).
2. The objects that can be set are **names of the dimension table, dimension, measure, calculated measure, and hierarchy**. If a dimension is defined with a translation, then it will also be applied in the hierarchy containing the dimension.
3. If the language in the analysis application client is not defined in the dataset translation, the dataset will use the default language. When using Excel, Excel will pass the LCID as language settings in Excel. For Tableau users, the language settings of the Windows system will be applied, so you can change the translations of the dataset displayed in Tableau by changing the language setting of the Windows system.
4. When creating a new calculated measure, please reference the measure in the original name, instead of the translation in the MDX expression.

#### How to set translation<a id="How to set translation"></a>

1. When editing a dataset, go to translation, click the globe icon to add a language.
   ![add language](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_translation.png)

2. Choose a language in the new column and click confirm.
   ![add language](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_translation02.png)

3. In this new group of translation, the user can input the translation of table names, dimensions or measures.
   ![add language](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_translation03.png)The above translation defines the Chinese-English translation of the year and month.

### Edit Dimension Usage<a id="Edit Dimension Usage"></a>

Since Kylin does not support complex many-to-many relationships, so there is no special definition here.

### Save and Create<a id="Save and Create"></a>

Now that you have completed the main work of creating a data set in this tutorial, click the **OK** button.

![image-20220310144027383](/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_save.png)

## Analysis in Excel<a id="Analysis in Excel"></a>

We have completed the definition of the data set, and now we can connect to Excel for analysis.

1. Select **Data —> From Analysis Services**
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel.png" alt="mdx excel" style="zoom:50%;" />

2. Next you need to fill in the address information of MDX for Kylin in the **Server Name** column. The sample is as follows:
   ```http://{host}:{port}/mdx/xmla/{project}```
   The default port of MDX for Kylin is 7080.
   The username and password are your MDX for Kylin's login username and password.
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_02.png" alt="mdx excel" style="zoom:50%;" />

3. Now the Dataset is connected to Excel. Select the previously created dataset ***Kylin_Sales_Analysis\*** and click Next.
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_03.png" alt="mdx excel" style="zoom:50%;" />

4. Check **Always attempt to use this file to refresh the data** and click Finish.
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_04.png" alt="mdx excel" style="zoom:50%;" />

5. Now you can analyze MDX for Kylin's dataset using the Excel PivotTable.
   <img src="/images/tutorial/4.0/Quick-Start-For-Mdx/mdx_excel_05.png" alt="mdx excel" style="zoom:50%;" />

## Manual & Source Code<a id="Manual & Source Code"></a>

More details about usage of MDX for Kylin, please access to [Manual of MDX for Kylin](https://kyligence.github.io/mdx-kylin/).

Please access to [https://github.com/Kyligence/mdx-kylin](https://github.com/Kyligence/mdx-kylin) to get source code.
