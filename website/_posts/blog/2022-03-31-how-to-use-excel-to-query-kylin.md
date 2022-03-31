---
layout: post-blog
title: How to use Excel to query Kylin? MDX for Kylin!
date: 2022-03-31 11:00:00
author: Xiaoxiang Yu
categories: blog
---

## **Abstract**

During the [Kylin community discussion](https://lists.apache.org/thread/4fkhyw1fyf0jg5cb18v7vxyqbn6vm3zv) at the beginning of this year, we talked about the positioning of multidimensional databases and the idea of building a Kylin-based business semantic layer. After some development efforts, we are delighted to announce the beta release of the **MDX** **for Kylin** **, an MDX query engine for Apache Kylin** to allow Kylin users to use **Excel** for data analysis.

#### **Target audiences**

- Kylin users who are not familiar with **MDX**

- Data engineers who are interested in building a **metrics platform** based on Kylin

- Data analysts who are interested in massive data analysis with **Excel**

#### **What you will learn**

- Basic concepts of MDX and MDX for Kylin

- Quickstart tutorial for MDX for Kylin

- Demonstration of how to use MDX for Kylin to define complex business metrics



## **Why Kylin need MDX?**

### **Multidimensional database and business semantic layer**

The primary difference between multidimensional databases and relational databases lies in business semantics. As the must-have skill of data analysts, SQL (Structured Query Language) is extremely expressive, but if we are talking in the context of "every professional will be an analyst", it is still too complex for non-technical users. For them, data lakes and data warehouses are like dark rooms that hold a huge amount of data; they cannot see, understand, or use the data for lack of the fundamental knowledge of databases and SQL syntax.

How to make data lakes and data warehouses **"easy"** for a non-technical user to use? One solution is to introduce a more user-friendly **"relational data model - multidimensional data model"**. If relational models are to provide a technique-oriented description of the data, multidimensional models intend to provide a business-oriented description of the data. In multidimensional databases, measures correspond to the business metrics that everyone is familiar with. Measures provide the analytic perspective to check and compare these business metrics. For example, it is like comparing the KPIs between this month and last month, or the performance of different business departments. By mapping the relational model to a multidimensional model, we add a business semantic layer on top of the technical data, thus helping non-technical users understand, explore, and use data.

In Kylin Roadmap, support to multidimensional query languages (such as MDX and DAX) is an important part, as we aim to enhance the business semantic capability of Kylin as a multi-dimensional database. Users can use MDX to convert the Kylin data model into business-friendly language, so they can perform multidimensional analysis with Excel, Tableau and other BI tools and understand the business values from their data. 

### **Build a business metrics platform with MDX**

When building complex business metrics, MDX provides the following advantages if compared to SQL:

1. **Better support for complex analysis scenarios**, such as semi-accumulation, many-to-many, and time window analysis;
2. **More BI support**: "Kylin + MDX" can be exposed as relational database tables through the SQL interface, or XMLA-compliant data source with business semantics. It allows MDX queries and integration with Excel and other BI tools through the XMLA protocol;
3. **Flexible defining of MDX semantic model based on Kylin data model**, it will convert the underlying data structure into a business-friendly language and add business value to data. With MDX model, we offer users a unified business semantic layer, they no longer need to worry about the underlying technology or implementation complexity when analyzing data. For more information, see *[The future of Apache Kylin](https://lists.apache.org/thread/4fkhyw1fyf0jg5cb18v7vxyqbn6vm3zv)*, *[SSAS Disadvantages: Opportunities for SSAS in the Cloud Era](https://kyligence.io/blog/opportunities-for-ssas-in-the-cloud/)*, and *[Semantic Layer: The BI Trend You Don’t Want to Miss](https://kyligence.io/blog/semantic-layer-the-bi-trend-you-dont-want-to-miss-in-2020/)**.* 

### **MDX Overview**

#### **What is MDX?**

MDX (Multi Dimensional eXpression) is a query language for OLAP Cube. It was first introduced by Microsoft in 1997 as part of the OLEDB for OLAP specification and later integrated into SSAS. Since then, it has been widely adopted by OLAP databases.

MDX is similar to SQL in many ways and also offers some SQL features though maybe not as intuitive or effective as SQL. For example, you can include SELECT, FROM, or WHERE clause in your MDX queries. But it is not an extension of SQL. You can use these keywords to dig into specific parts of the Cube. 

[MDX query syntax ](https://docs.microsoft.com/en-us/sql/mdx/mdx-syntax-elements-mdx?view=sql-server-ver15)are as follows: 

```sql
select <axis_specification>[, <axis_specification>]
 from <cube_specification>
 where <slicer_specification>
```

#### **Key concepts of MDX**

Please learn some basic MDX concepts before we continue. 

1. Dimensions, Levels, Members, and Measures
2. Cell, Tuple, and Set
3. Query Axis and Slicer Axis

For detailed information about these concepts, see [MDX Syntax Elements (MDX)](https://docs.microsoft.com/en-us/sql/mdx/mdx-syntax-elements-mdx?view=sql-server-ver15).

#### **Comparison of MDX and SQL**

The query objects are different. MDX is to query the cube, with data already joined and aggregated, so users needn't specify the join relation when querying. SQL is to query a table with detailed records. Users need to specify the join relation among the tables when querying.

[Another difference is the query result](https://dba.stackexchange.com/questions/138311/good-example-of-mdx-vs-sql-for-analytical-queries). SQL returns a 2d data subset, while MDX returns the cubes. 

### **MDX for Kylin** **Overview**

#### **What is** **MDX for Kylin**?

**MDX for Kylin** is an MDX query engine which developed based on **Mondrian**, contributed by **Kyligence,** and with **Apache Kylin** as data source. Like Microsoft SSAS, MDX for Kylin can also integrate many data analysis tools, including Microsoft Excel and Tableau, to provide a better user experience for big data analysis.

#### **How** **to create business metrics**

##### **Atomic metrics and business metrics**

In Kylin Cube, we will perform certain aggregate calculations (such as Sum/Max/Min/Count/Count Distinct, exclude TopN) on a single column when creating measures, and the measures created are called atomic metrics. 

In actual business scenarios, we can run complex calculations based on these atomic metrics to create composite metrics with business implications, and these metrics are called business metrics.

##### **Hierarchy, Calculated Measure, and NamedSet**

**Hierarchy:** Hierarchies are collections of dimension-based hierarchies that can empower data analysts with advanced analytical capabilities. For example, you can create a time hierarchy with year, quarter, month, week, and day as its hierarchy. Then data analysts can do a YOY analysis on the sales volume, or dig into the "Quarter > Month > Week > Day" hierarchy for more detailed analysis.

**Calculated Measure:** Calculated Measure are metrics/indexes acquired by running composite computing on the **atomic metrics** with MDX expressions. We mainly use calculated measures to create **business metrics**.

**NamedSet:** Namedset is for the scenario when you need to reuse a set of members in MDX for Kylin. A NamedSet uses specified expressions to get the set members. It can be placed directly on the axis or used in expressions of Calculated Measure for or other Namedset.

#### **Dataset as** **semantic model**

In Kylin 4, we create a data model based on the relationship among tables, and define different dimensions and measures on the Cube. These measures are **atomic metrics**. 

In MDX for Kylin, we join related Kylin Cubes to create datasets and create **business metrics** based on atomic metrics.

![](/images/blog/how_to_use_excel_to_query_kylin/1_use_excel_to_query_kylin.en.png)

#### **Process of calculating**

The client(BI/Excel) sends an MDX query to MDX for Kylin, which will then be parsed into SQL and sent to Kylin. After that, Kylin will answer the SQL query based on the pre-computed Cuboid and return the result to MDX for Kylin. Then, MDX for Kylin will do some derived metrics calculation, and return the multidimensional data results to the client.

![](/images/blog/how_to_use_excel_to_query_kylin/2_use_excel_to_query_kylin.en.png)

#### **Summary**

**MDX for** **Kylin** supports MDX interface enhancing the semantic capability and creates a unified data analysis and management user experience. Now users can better leverage the value of data. The figure below shows the process of how raw data is processed into business metrics.

![](/images/blog/how_to_use_excel_to_query_kylin/3_use_excel_to_query_kylin.en.png)

### **Technical advantages of** **MDX for Kylin**

If compared with other open-source MDX query engines, MDX for Kylin has the following advantages:

- Better support to BIs (Excel/Tableau/Power BI, etc.)and compliance with XMLA protocol

- Optimize the MDX Query for BIs

- Accelerate MDX queries with Kylin's pre-computing capability

- Easy-to-use interface for metrics definition and management



## **Quick start with Docker**

#### **Test environment**

- MacBook Pro: Docker Desktop (latest version)
- Windows 10 virtual machine: Microsoft Excel (for Windows)

#### **Start the container**

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

#### Environment Check

Wait for a few minutes, then check if you can visit the web UI of HDFS, YARN, Kylin, and MDX for Kylin.

| **Components** | **Web UI**                            |
| -------------- | ------------------------------------- |
| HDFS           | http://localhost:50070/dfshealth.html |
| YARN           | http://localhost:8088/cluster         |
| Kylin          | http://localhost:7070/kylin           |
| MDX for Kylin  | http://localhost:7080/overview        |

#### Build a sample Cube

In this tutorial, we will use Kylin's built-in sample Cube: `kylin_sales_cube`

![](/images/blog/how_to_use_excel_to_query_kylin/4_use_excel_to_query_kylin.en.png)

#### Create a MDX dataset

##### **Log in to** **MDX for Kylin**

Log in to MDX for Kylin through the web UI: http://localhost:7080. The default account/password is **ADMIN/KYLIN**, the same as Kylin.

![](/images/blog/how_to_use_excel_to_query_kylin/5_use_excel_to_query_kylin.en.png)

##### Define the dataset and relations

![](/images/blog/how_to_use_excel_to_query_kylin/6_use_excel_to_query_kylin.en.png)

![](/images/blog/how_to_use_excel_to_query_kylin/7_use_excel_to_query_kylin.en.png)

##### **Create a time hierarchy**

- Configure the dimension table `KYLIN_CAL_DT`. 

![](/images/blog/how_to_use_excel_to_query_kylin/8_use_excel_to_query_kylin.en.png)

- Configure the column `YEAR_BEG_DT`, and set **Type** to **Year**.

![](/images/blog/how_to_use_excel_to_query_kylin/9_use_excel_to_query_kylin.en.png)

![](/images/blog/how_to_use_excel_to_query_kylin/10_use_excel_to_query_kylin.en.png)

- Configure `MONTH_BEG_DT`and `WEEK_BEG_DT`, set them to the correspondent hierarchy. 
- Create a time hierarchy `Calendar`. Please be noted the time hierarchy should be in a "year-month-week" order.

![](/images/blog/how_to_use_excel_to_query_kylin/11_use_excel_to_query_kylin.en.png)

##### **Rename the atomic metrics**

Rename the atomic metric `GMV_SUM` to some names with business implications. In this tutorial, we named it as **Sales volume**, and renamed `SELLER_CNT_HLL` as **Retailer numbers**.

![](/images/blog/how_to_use_excel_to_query_kylin/12_use_excel_to_query_kylin.en.png)

##### **Create business metrics (calculated measures)**

- Create the business metric **Av****erage sales volume of** **retailers**.

![](/images/blog/how_to_use_excel_to_query_kylin/13_use_excel_to_query_kylin.en.png)

- Build "YoY growth rate of sales volume" and "MoM growth rate of sales volume" sequentially. 

| Metrics                                 | **MDX Expression**                                           |
| --------------------------------------- | ------------------------------------------------------------ |
| Average sales volume of retailers       | [Measures].[Sales volume]/[Measures].[Retailer number]       |
| YoY growth rate of sales volume         | [Measures].[Sales volume] / SUM(  ParallelPeriod(    [KYLIN_CAL_DT].[Calendar-Hierarchy].[YEAR_BEG_DT],    1,    [KYLIN_CAL_DT].[Calendar-Hierarchy].CurrentMember  ),  [Measures].[Sales volume] ) - 1 |
| MoM growth rate of sales volume         | [Measures].[Sales volume] / SUM(  ParallelPeriod(    [KYLIN_CAL_DT].[Calendar-Hierarchy].[MONTH_BEG_DT],    1,    [KYLIN_CAL_DT].[Calendar-Hierarchy].CurrentMember  ),  [Measures].[Sales volume] ) - 1 |
| Total sales volume                      | Fixed([KYLIN_CAL_DT].[YEAR_BEG_DT], [Measures].[Sales volume]) |
| Proportion in total annual sales volume | [Measures].[Sales volume]/[Measures].[Total sales volume]    |

#### **Access business metrics through Excel pivot tables**

##### **Connect** **MDX for Kylin****!**

- Open Microsoft Excel (for Windows)

![](/images/blog/how_to_use_excel_to_query_kylin/14_use_excel_to_query_kylin.en.png)

- Configure the MDX for Kylin server address. Please update the IP_Adress with your IP address, and use Kylin's account and password(ADMIN/KYLIN in this case).

![](/images/blog/how_to_use_excel_to_query_kylin/15_use_excel_to_query_kylin.en.png)

##### Check sales volume with pivot tables

-  Configure pivot table

![](/images/blog/how_to_use_excel_to_query_kylin/16_use_excel_to_query_kylin.en.png)

- Check YoY growth rate of sales volume

![](/images/blog/how_to_use_excel_to_query_kylin/17_use_excel_to_query_kylin.en.png)

- Check MoM growth rate of sales volume

![](/images/blog/how_to_use_excel_to_query_kylin/18_use_excel_to_query_kylin.en.png)


#### Call API to query MDX for Kylin

If you do NOT have a windows version Excel, you can also use REST API to test the business metrics just created.  Note: please change the variables in the `<Statement>` `</Statement>` section based on your setting and update the value of `Catalog` if needed. 

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
SELECT {[Measures].[Sales volume],
        [Measures].[Retailer number],
        [Measures].[Average sales volume of retailers],
        [Measures].[Proportion in total annual sales volume],
        [Measures].[YoY growth rate of sales volume],
        [Measures].[MoM growth rate of sales volume]} 
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

## Contact us

If you want to check the official documentation, please check the manual : https://kyligence.github.io/mdx-kylin/en. For developers who want to contribute, please check our Github page : https://github.com/Kyligence/mdx-kylin .

Feel free to leave your suggestion, ask a question or report a bug by referring https://kyligence.github.io/mdx-kylin/en/contact/ . 
