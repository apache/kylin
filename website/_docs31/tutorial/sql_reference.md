---
layout: docs31
title:  SQL Reference
categories: tutorial
permalink: /docs31/tutorial/sql_reference.html
---
   
Apache Kylin relies on Apache Calcite to parse and optimize the SQL statements. As an OLAP engine, Kylin supports `SELECT` statements, while doesn't support others like `INSERT`, `UPDATE` and `DELETE` operations in SQL, so Kylin's SQL grammer is a subset of Apache Calcite. This page lists the SQL grammar, the functions and the basic data types that Kylin supports. You can also check [Calcite SQL reference](https://calcite.apache.org/docs/reference.html) for more detailed info. 
   
## Grammar

[QUERY SYNTAX](#QUERYSYNTAX)
[SELECT](#SELECT)
　[STATEMENT](#STATEMENT)
　[EXPRESSION](#EXPRESSION)
[SUBQUERY](#SUBQUERY)
[JOIN](#JOIN)
　[INNER JOIN](#INNERJOIN)
　[LEFT JOIN](#LEFTJOIN)
[UNION](#UNION)
[UNION ALL](#UNIONALL)

## Function

[COUNT](#COUNT)
　[COUNT(COLUMN)](#COUNT_COLUMN)
　[COUNT(*)](#COUNT_)
[COUNT_DISTINCT](#COUNT_DISTINCT)
[MAX](#MAX)
[MIN](#MIN)
[PERCENTILE](#PERCENTILE)
[SUM](#SUM)
[TOP_N](#TOP_N)
[WINDOW](#WINDOW)
　[ROW_NUMBER](#ROW_NUMBER)
　[AVG](#AVG)
　[RANK](#RANK)
　[DENSE_RANK](#DENSE_RANK)
　[FIRST_VALUE](#FIRST_VALUE)
　[LAST_VALUE](#LAST_VALUE)
　[LAG](#LAG)
　[LEAD](#LEAD)
　[NTILE](#NTILE)
　[CASE WHEN](#CASEWHEN)
　[CAST](#CAST)
[SUSTRING](#SUBSTRING)
[COALESCE](#COALESCE)

## Data Type

[DATA TYPE](#DATATYPE)


## QUERY SYNTAX {#QUERYSYNTAX}
{% highlight Groff markup %}
statement:
|  query

query:
      values
  |  WITH withItem [ , withItem ]* query
  |   {
          select
      |  selectWithoutFrom
      |  query UNION [ ALL | DISTINCT ] query
      |  query INTERSECT [ ALL | DISTINCT ] query
      }
      [ ORDER BY orderItem [, orderItem ]* ]
      [ LIMIT { count | ALL } ]
      [ OFFSET start { ROW | ROWS } ]
      [ FETCH { FIRST | NEXT } [ count ] { ROW| ROWS } ]

withItem:
      name
      ['(' column [, column ]* ')' ]
      AS '(' query ')'

orderItem:
      expression [ ASC | DESC ]［ NULLS FIRST |NULLS LAST ］

select:
      SELECT [ ALL | DISTINCT]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY { groupItem [, groupItem ]* }]
      [ HAVING booleanExpression ]
      [ WINDOW windowName AS windowSpec [,windowName AS windowSpec ]* ]

selectWithoutFrom:
      SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

projectItem:
      expression [ [ AS ] columnAlias ]
  |  tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |  tableExpression [ NATURAL ]［( LEFT | RIGHT | FULL ) [ OUTER ] ］ JOIN tableExpression [ joinCondition ]

joinCondition:
      ON booleanExpression
  |  USING '(' column [, column ]* ')'

tableReference:
      tablePrimary
      [ matchRecognize ]
      [ [ AS ] alias [ '(' columnAlias [,columnAlias ]* ')' ] ]

tablePrimary:
      [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName. ] tableName ')'
  |   [ LATERAL ] '(' query ')'
  |  UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]*')' ')'

values:
      VALUES expression [, expression ]*

groupItem:
      expression
  |   '('')'
  |   '('expression [, expression ]* ')'
  |  GROUPING SETS '(' groupItem [, groupItem ]* ')'

windowRef:
      windowName
  |  windowSpec

windowSpec:
      [windowName ]
      '('
      [ ORDER BY orderItem [, orderItem ]* ]
      [ PARTITION BY expression [, expression]* ]
      [
          RANGE numericOrIntervalExpression { PRECEDING | FOLLOWING }
      |  ROWS numericExpression { PRECEDING | FOLLOWING }
      ]
    ')'
 
{% endhighlight %}
 
## SELECT {#SELECT}

### STATEMENT {#STATEMENT}
```SELECT``` chooses the data from the table. ```COUNT``` is used for quantitative statistics. ```DISTINCT``` filters out duplicate results. ```AS``` is used to alias tables or columns. ```FROM``` identifies the table being queried. ```JOIN``` is used to connect two tables to get the desired data. ```WHERE``` is used to specify the standard of selection. ```LIKE``` is used to search for a specified pattern in a column in a ```WHERE``` clause. ```BETWEEN ... AND``` is used to select a range of data between two values. ```AND``` and ```OR``` is used to filter records based on more than one condition. ```GROUP BY``` groups the result by the given expression(s). ```HAVING``` filters rows after grouping. ```ORDER BY``` sorts the result based on the given expression, usually uses with ```TOPN``` function. ```LIMIT``` limits the number of rows returned by the query.

Example:
{% highlight Groff markup %}
SELECT COUNT(*) FROM kylin_sales;

SELECT COUNT(DISTINCT seller_id) FROM kylin_sales;

SELECT seller_id, COUNT(1) FROM kylin_sales GROUP BY seller_id;

SELECT lstg_format_name, SUM(price) AS gmv, COUNT(DISTINCT seller_id) AS dist_seller FROM kylin_sales WHERE lstg_format_name='FP-GTC' GROUP BY lstg_format_name HAVING COUNT(DISTINCT seller_id) > 50;

SELECT lstg_format_name FROM kylin_sales INNER JOIN kylin_cal_dt AS kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt WHERE NOT(lstg_format_name NOT LIKE '%ab%') GROUP BY lstg_format_name;

SELECT kylin_cal_dt.cal_dt FROM kylin_sales INNER JOIN kylin_cal_dt AS kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt WHERE kylin_cal_dt.cal_dt BETWEEN DATE '2013-01-01' AND DATE '2013-06-04';

SELECT kylin_sales.lstg_format_name, SUM(kylin_sales.price) AS gmv, COUNT(*) AS trans_cnt FROM kylin_sales WHERE kylin_sales.lstg_format_name IS NULL GROUP BY kylin_sales.lstg_format_name HAVING SUM(price)>5000 AND COUNT(*)>72; 

SELECT kylin_sales.lstg_format_name, SUM(kylin_sales.price) AS gmv, COUNT(*) AS trans_cnt FROM kylin_sales WHERE kylin_sales.lstg_format_name IS NOT NULL GROUP BY kylin_sales.lstg_format_name HAVING SUM(price)>5000 OR COUNT(*)>20;

SELECT lstg_format_name, SUM(price) AS gmv, COUNT(1) AS trans_cnt FROM kylin_sales WHERE lstg_format_name='FP-GTC' GROUP BY lstg_format_name ORDER BY lstg_format_name LIMIT 10; 
{% endhighlight %}

### EXPRESSION {#EXPRESSION}
An expression in a ```SELECT``` statement. All columns in a table may be selected using *.
Example:
1. *
2. ID AS VALUE
3. VALUE + 1

## SUBQUERY {#SUBQUERY}
Example:
{% highlight Groff markup %}
SELECT cal_dt ,sum(price) AS sum_price FROM (SELECT kylin_cal_dt.cal_dt, kylin_sales.price FROM kylin_sales INNER JOIN kylin_cal_dt AS kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt INNER JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id) t GROUP BY cal_dt;
{% endhighlight %}

## JOIN {#JOIN}

### INNER JOIN {#INNERJOIN}
The ```INNER JOIN``` keyword returns rows when there is at least one match in the table.

Example:
{% highlight Groff markup %}
SELECT kylin_cal_dt.cal_dt, kylin_sales.price FROM kylin_sales INNER JOIN kylin_cal_dt AS kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt;
{% endhighlight %}

### LEFT JOIN {#LEFTJOIN}
The ```LEFT JOIN``` keyword returns all rows from the left table (kylin_sales), even if there are no matching rows in the right table (kylin_category_groupings).

Example:
{% highlight Groff markup %}
SELECT seller_id FROM kylin_sales LEFT JOIN kylin_category_groupings AS kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id WHERE lstg_format_name='FP-GTC' GROUP BY seller_id LIMIT 20;
{% endhighlight %}

## UNION {#UNION}
The ```UNION``` operator is used to combine the result sets of two or more ```SELECT``` statements.
*Note that* the ```SELECT``` statement inside ```UNION``` must have the same number of columns. Columns must also have similar data types. At the same time, the order of the columns in each ```SELECT``` statement must be the same.
By default, the ```UNION``` operator picks a different value. If you allow duplicate values, use ```UNION ALL```.

Example:
{% highlight Groff markup %}
SELECT SUM(price) AS x, seller_id, COUNT(*) AS y FROM kylin_sales WHERE part_dt < DATE '2012-08-01' GROUP BY seller_id UNION (SELECT SUM(price) AS x, seller_id, COUNT(*) AS y FROM kylin_sales WHERE part_dt > DATE '2012-12-01' GROUP BY seller_id);
{% endhighlight %}

## UNION ALL {#UNIONALL}
The ```UNION ALL``` command is almost equivalent to the ```UNION``` command, but the ```UNION ALL``` command lists all values.

Example:
{% highlight Groff markup %}
SELECT COUNT(trans_id) AS trans_id FROM kylin_sales AS test_a WHERE trans_id <> 1 UNION ALL SELECT COUNT(trans_id) AS trans_id FROM kylin_sales AS test_b;
{% endhighlight %}

## COUNT {#COUNT}
Returns the number of rows matching the specified criteria.

### COUNT(COLUMN) {#COUNT_COLUMN}
Example:
{% highlight Groff markup %}
SELECT COUNT(seller_id) FROM kylin_sales;
{% endhighlight %}

### COUNT(*) {#COUNT_}
Example:
{% highlight Groff markup %}
SELECT COUNT(*) FROM kylin_sales;
{% endhighlight %}


## COUNT_DISTINCT {#COUNT_DISTINCT}
Example:
{% highlight Groff markup %}
SELECT COUNT(DISTINCT seller_id) AS DIST_SELLER FROM kylin_sales;
{% endhighlight %}

## MAX {#MAX}
Returns the maximum value in a column. NULL values are not included in the calculation.
Example:
{% highlight Groff markup %}
SELECT MAX(lstg_site_id) FROM kylin_sales;
{% endhighlight %}


## MIN {#MIN}
Returns the minimum value in a column. NULL values are not included in the calculation.
Example:
{% highlight Groff markup %}
SELECT MIN(lstg_site_id) FROM kylin_sales;
{% endhighlight %}


## PERCENTILE {#PERCENTILE}
Example:
{% highlight Groff markup %}
SELECT seller_id, PERCENTILE(price, 0.5) FROM kylin_sales GROUP BY seller_id;

SELECT seller_id, PERCENTILE_APPROX(price, 0.5) FROM kylin_sales GROUP BY seller_id;
{% endhighlight %}


## SUM {#SUM}
Returns the total number of numeric columns.
Example:
{% highlight Groff markup %}
SELECT SUM(price) FROM kylin_sales;
{% endhighlight %}

## TOP_N {#TOP_N}
Example:
{% highlight Groff markup %}
SELECT SUM(price) AS gmv
 FROM kylin_sales 
INNER JOIN kylin_cal_dt AS kylin_cal_dt
 ON kylin_sales.part_dt = kylin_cal_dt.cal_dt
 INNER JOIN kylin_category_groupings
 ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id
 WHERE kylin_cal_dt.cal_dt between DATE '2013-09-01' AND DATE '2013-10-01' AND (lstg_format_name='FP-GTC' OR 'a' = 'b')
 GROUP BY kylin_cal_dt.cal_dt;
 
SELECT kylin_sales.part_dt, seller_id
FROM kylin_sales
INNER JOIN kylin_cal_dt AS kylin_cal_dt
ON kylin_sales.part_dt = kylin_cal_dt.cal_dt
INNER JOIN kylin_category_groupings
ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id
AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id 
GROUP BY 
kylin_sales.part_dt, kylin_sales.seller_id ORDER BY SUM(kylin_sales.price) DESC LIMIT 20;
{% endhighlight %}

## WINDOW {#WINDOW}
The ```WINDOW``` function performs the calculation on a set of table rows associated with the current row.
*NOTE*: ```OVER``` clause is necessary for window functions.

### ROW_NUMBER {#ROW_NUMBER}
Example:
{% highlight Groff markup %}
SELECT lstg_format_name, SUM(price) AS gmv, ROW_NUMBER() OVER() FROM kylin_sales GROUP BY lstg_format_name;
{% endhighlight %}

### AVG {#AVG}
Returns the average of the numeric columns. NULL values are not included in the calculation.
Example:
{% highlight Groff markup %}
SELECT lstg_format_name, AVG(SUM(price)) OVER(PARTITION BY lstg_format_name) FROM kylin_sales GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### RANK {#RANK}
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price), RANK() OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "rank" FROM kylin_sales GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### DENSE_RANK {#DENSE_RANK}
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price), DENSE_RANK() OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "dense_rank" FROM kylin_sales GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### FIRST_VALUE {#FIRST_VALUE}
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, FIRST_VALUE(SUM(price)) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "first" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### LAST_VALUE {#LAST_VALUE}
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, LAST_VALUE(SUM(price)) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "current" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### LAG {#LAG}
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, LAG(SUM(price), 1, 0.0) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "prev" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### LEAD {#LEAD}
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, LEAD(SUM(price), 1, 0.0) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "next" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### NTILE {#NTILE}
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, NTILE(4) OVER (PARTITION BY lstg_format_name ORDER BY part_dt) AS "quarter" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### CASE WHEN {#CASEWHEN}
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, (CASE LAG(SUM(price), 1, 0.0) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) WHEN 0.0 THEN 0 ELSE SUM(price)/LAG(SUM(price), 1, 0.0) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) END) AS "prev" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### CAST {#CAST}
The keyword ```RANGE```, ```INTERVAL``` to specify a range. ```PRECEDING``` means the first few days (second/minute/hour/month/year). ```FOLLOWING``` means the next few days (second/minute/hour/month/year).
Example:
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, FIRST_VALUE(SUM(price)) OVER (PARTITION BY lstg_format_name ORDER BY CAST(part_dt AS timestamp) RANGE INTERVAL '3' DAY PRECEDING) AS "prev 3 days", LAST_VALUE(SUM(price)) OVER (PARTITION BY lstg_format_name ORDER BY CAST(part_dt AS timestamp) RANGE INTERVAL '3' DAY FOLLOWING) AS "next 3 days" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

## SUBSTRING {#SUBSTRING}
Example:
{% highlight Groff markup %}
SELECT SUBSTRING(lstg_format_name, 1) FROM kylin_sales;
{% endhighlight %}
	
## COALESCE {#COALESCE}
Example:
{% highlight Groff markup %}
SELECT COALESCE(lstg_format_name, '888888888888') FROM kylin_sales;
{% endhighlight %}


## DATA TYPE {#DATATYPE}

| ---------- | ---------- | ---------- | ---------- | -------------------- |
| ANY        | CHAR       | VARCHAR    | STRING     | BOOLEAN              |
| BYTE       | BINARY     | INT        | SHORT      | LONG                 |
| INTEGER    | TINYINT    | SMALLINT   | BIGINT     | TIMESTAMP            |
| FLOAT      | REAL       | DOUBLE     | DECIMAL    | DATETIME             |
| NUMERIC    | DATE       | TIME       |            |                      |