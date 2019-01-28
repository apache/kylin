---
layout: docs-cn
title:  "SQL 语法"
categories: tutorial
permalink: /cn/docs/tutorial/kylin_grammar.html
---

## 语法

[SELECT](#SELECT)
　[STATEMENT](#STATEMENT)
　[EXPRESSION](#EXPRESSION)
[SUBQUERY](#SUBQUERY)
[JOIN](#JOIN)
　[INNER JOIN](#INNERJOIN)
　[LEFT JOIN](#LEFTJOIN)
　[CROSS JOIN](#CROSSJOIN)
[UNION](#UNION)
[UNION ALL](#UNIONALL)

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

### CROSS JOIN {#CROSSJOIN}
例子：
{% highlight Groff markup %}
SELECT * FROM (SELECT leaf_categ_id, SUM(price) AS sum_price FROM kylin_sales GROUP BY leaf_categ_id UNION ALL SELECT leaf_categ_id, SUM(price) AS sum_price FROM kylin_sales GROUP BY leaf_categ_id) CROSS JOIN (SELECT SUM(price) AS sum_price_2 FROM kylin_sales GROUP BY leaf_categ_id) UNION ALL SELECT CAST(1999 AS bigint) AS leaf_categ_id, 11.2 AS sum_price, 21.2 AS sum_price2 UNION ALL SELECT * FROM (SELECT leaf_categ_id, SUM(price) AS sum_price FROM kylin_sales GROUP BY leaf_categ_id UNION ALL SELECT leaf_categ_id, SUM(price) AS sum_price FROM kylin_sales GROUP BY leaf_categ_id) CROSS JOIN (SELECT SUM(price) AS sum_price_2 FROM kylin_sales GROUP BY leaf_categ_id) ORDER BY 1;
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
