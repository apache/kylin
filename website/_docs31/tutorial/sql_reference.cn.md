---
layout: docs31-cn
title:  "SQL 快速参考"
categories: tutorial
permalink: /cn/docs31/tutorial/sql_reference.html
---

Apache Kylin 使用 Apache Calcite 做 SQL 解析和优化。作为一款 OLAP 引擎, Kylin 支持 `SELECT` 操作，而不支持其它操作例如 `INSERT`，`UPDATE` 和 `DELETE`，因此 Kylin 的 SQL 语法是 Apache Calcite 支持语法的一个子集。本文列举了 Kylin 支持的 SQL 语法、函数以及数据类型，但可能并不完整。您可以查看 [Calcite SQL reference](https://calcite.apache.org/docs/reference.html) 以了解更多内容。 

## 语法
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

## 函数

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

## 数据类型
[数据类型](#datatype)

## 查询语法 {#QUERYSYNTAX}
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
```SELECT``` 用于从表中选取数据。```COUNT``` 用于统计数据。```DISTINCT``` 过滤掉重复的结果。```AS``` 用于给表或列起别名。```FROM``` 指定要查询的表。```JOIN``` 用于连接两个表以获取所需的数据。```WHERE``` 用于规定选择的标准。```LIKE``` 用于在 ```WHERE``` 子句中搜索列中的指定模式。```BETWEEN ... AND``` 选取介于两个值之间的数据范围。```AND``` 和 ```OR``` 用于基于一个以上的条件对记录进行过滤。```GROUP BY``` 按给定表达式对结果进行分组。```HAVING``` 用于分组后过滤行。```ORDER BY``` 用于对结果集进行排序，通常和 ```TOPN``` 一起使用。```LIMIT``` 用来限制查询返回的行数。

例子：
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
在 ```SELECT``` 语句中的表达式。 可以使用 * 选择表中的所有列。
例子：
1. *
2. 将 ID 作为值
3. 值 + 1

## SUBQUERY {#SUBQUERY}
例子：
{% highlight Groff markup %}
SELECT cal_dt ,sum(price) AS sum_price FROM (SELECT kylin_cal_dt.cal_dt, kylin_sales.price FROM kylin_sales INNER JOIN kylin_cal_dt AS kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt INNER JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id) t GROUP BY cal_dt;
{% endhighlight %}

## JOIN {#JOIN}

### INNER JOIN {#INNERJOIN}
在表中存在至少一个匹配时，```INNER JOIN``` 关键字返回行。
例子：
{% highlight Groff markup %}
SELECT kylin_cal_dt.cal_dt, kylin_sales.price FROM kylin_sales INNER JOIN kylin_cal_dt AS kylin_cal_dt ON kylin_sales.part_dt**** = kylin_cal_dt.cal_dt;
{% endhighlight %}

### LEFT JOIN {#LEFTJOIN}
使用 ```LEFT JOIN``` 关键字会从左表 (kylin_sales) 那里返回所有的行，即使在右表 (kylin_category_groupings) 中没有匹配的行。
例子：
{% highlight Groff markup %}
SELECT seller_id FROM kylin_sales LEFT JOIN kylin_category_groupings AS kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id WHERE lstg_format_name='FP-GTC' GROUP BY seller_id LIMIT 20;
{% endhighlight %}

## UNION {#UNION}
```UNION``` 操作符用于合并两个或多个 ```SELECT``` 语句的结果集。
*注意* ```UNION``` 内部的 ```SELECT``` 语句必须拥有相同数量的列。列也必须拥有相似的数据类型。同时，每条 ```SELECT``` 语句中的列的顺序必须相同。
默认地，```UNION``` 操作符选取不同的值。如果允许重复的值，请使用 ```UNION ALL```。

例子：
{% highlight Groff markup %}
SELECT SUM(price) AS x, seller_id, COUNT(*) AS y FROM kylin_sales WHERE part_dt < DATE '2012-08-01' GROUP BY seller_id UNION (SELECT SUM(price) AS x, seller_id, COUNT(*) AS y FROM kylin_sales WHERE part_dt > DATE '2012-12-01' GROUP BY seller_id);
{% endhighlight %}

## UNION ALL {#UNIONALL}
```UNION ALL``` 命令和 ```UNION``` 命令几乎是等效的，不过 ```UNION ALL``` 命令会列出所有的值。

例子：
{% highlight Groff markup %}
SELECT COUNT(trans_id) AS trans_id FROM kylin_sales AS test_a WHERE trans_id <> 1 UNION ALL SELECT COUNT(trans_id) AS trans_id FROM kylin_sales AS test_b;
{% endhighlight %}

## COUNT {#COUNT}
用于返回与指定条件匹配的行数。

### COUNT(COLUMN) {#COUNT_COLUMN}
例子：
{% highlight Groff markup %}
SELECT COUNT(seller_id) FROM kylin_sales;
{% endhighlight %}

### COUNT(*) {#COUNT_}
例子：
{% highlight Groff markup %}
SELECT COUNT(*) FROM kylin_sales;
{% endhighlight %}


## COUNT_DISTINCT {#COUNT_DISTINCT}
例子：
{% highlight Groff markup %}
SELECT COUNT(DISTINCT seller_id) AS DIST_SELLER FROM kylin_sales;
{% endhighlight %}

## MAX {#MAX}
返回一列中的最大值。NULL 值不包括在计算中。
例子：
{% highlight Groff markup %}
SELECT MAX(lstg_site_id) FROM kylin_sales;
{% endhighlight %}


## MIN {#MIN}
返回一列中的最小值。NULL 值不包括在计算中。
例子：
{% highlight Groff markup %}
SELECT MIN(lstg_site_id) FROM kylin_sales;
{% endhighlight %}


## PERCENTILE {#PERCENTILE}
例子：
{% highlight Groff markup %}
SELECT seller_id, PERCENTILE(price, 0.5) FROM kylin_sales GROUP BY seller_id;

SELECT seller_id, PERCENTILE_APPROX(price, 0.5) FROM kylin_sales GROUP BY seller_id;
{% endhighlight %}


## SUM {#SUM}
返回数值列的总数。
例子：
{% highlight Groff markup %}
SELECT SUM(price) FROM kylin_sales;
{% endhighlight %}

## TOP_N {#TOP_N}
例子：
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
```WINDOW``` 函数在和当前行相关的一组表行上执行计算。
*注意*：```WINDOW``` 函数中必须有 ```OVER``` 子句

### ROW_NUMBER {#ROW_NUMBER}
例子：
{% highlight Groff markup %}
SELECT lstg_format_name, SUM(price) AS gmv, ROW_NUMBER() OVER() FROM kylin_sales GROUP BY lstg_format_name;
{% endhighlight %}

### AVG {#AVG}
返回数值列的平均值。NULL 值不包括在计算中。
例子：
{% highlight Groff markup %}
SELECT lstg_format_name, AVG(SUM(price)) OVER(PARTITION BY lstg_format_name) FROM kylin_sales GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### RANK {#RANK}
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price), RANK() OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "rank" FROM kylin_sales GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### DENSE_RANK {#DENSE_RANK}
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price), DENSE_RANK() OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "dense_rank" FROM kylin_sales GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### FIRST_VALUE {#FIRST_VALUE}
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, FIRST_VALUE(SUM(price)) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "first" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### LAST_VALUE {#LAST_VALUE}
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, LAST_VALUE(SUM(price)) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "current" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### LAG {#LAG}
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, LAG(SUM(price), 1, 0.0) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "prev" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### LEAD {#LEAD}
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, LEAD(SUM(price), 1, 0.0) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) AS "next" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### NTILE {#NTILE}
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, NTILE(4) OVER (PARTITION BY lstg_format_name ORDER BY part_dt) AS "quarter" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### CASE WHEN {#CASEWHEN}
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, (CASE LAG(SUM(price), 1, 0.0) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) WHEN 0.0 THEN 0 ELSE SUM(price)/LAG(SUM(price), 1, 0.0) OVER(PARTITION BY lstg_format_name ORDER BY part_dt) END) AS "prev" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

### CAST {#CAST}
```RANGE```，```INTERVAL``` 关键字指明了范围。```PRECEDING``` 表示前几天（秒/分/时/月/年）。```FOLLOWING``` 表示后几天（秒/分/时/月/年）。
例子：
{% highlight Groff markup %}
SELECT part_dt, lstg_format_name, SUM(price) AS gmv, FIRST_VALUE(SUM(price)) OVER (PARTITION BY lstg_format_name ORDER BY CAST(part_dt AS timestamp) RANGE INTERVAL '3' DAY PRECEDING) AS "prev 3 days", LAST_VALUE(SUM(price)) OVER (PARTITION BY lstg_format_name ORDER BY CAST(part_dt AS timestamp) RANGE INTERVAL '3' DAY FOLLOWING) AS "next 3 days" FROM kylin_sales WHERE part_dt < '2012-02-01' GROUP BY part_dt, lstg_format_name;
{% endhighlight %}

## SUBSTRING {#SUBSTRING}
例子：
{% highlight Groff markup %}
SELECT SUBSTRING(lstg_format_name, 1) FROM kylin_sales;
{% endhighlight %}
	
## COALESCE {#COALESCE}
例子：
{% highlight Groff markup %}
SELECT COALESCE(lstg_format_name, '888888888888') FROM kylin_sales;
{% endhighlight %}

## 数据类型 {#datatype}

| ---------- | ---------- | ---------- | ---------- | -------------------- |
| ANY        | CHAR       | VARCHAR    | STRING     | BOOLEAN              |
| BYTE       | BINARY     | INT        | SHORT      | LONG                 |
| INTEGER    | TINYINT    | SMALLINT   | BIGINT     | TIMESTAMP            |
| FLOAT      | REAL       | DOUBLE     | DECIMAL    | DATETIME             |
| NUMERIC    | DATE       | TIME       |            |                      |