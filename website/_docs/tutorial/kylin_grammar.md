---
layout: docs
title:  SQL Grammar
categories: tutorial
permalink: /docs/tutorial/kylin_grammar.html
---
   
## Grammar

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

### CROSS JOIN {#CROSSJOIN}
Example:
{% highlight Groff markup %}
SELECT * FROM (SELECT leaf_categ_id, SUM(price) AS sum_price FROM kylin_sales GROUP BY leaf_categ_id UNION ALL SELECT leaf_categ_id, SUM(price) AS sum_price FROM kylin_sales GROUP BY leaf_categ_id) CROSS JOIN (SELECT SUM(price) AS sum_price_2 FROM kylin_sales GROUP BY leaf_categ_id) UNION ALL SELECT CAST(1999 AS bigint) AS leaf_categ_id, 11.2 AS sum_price, 21.2 AS sum_price2 UNION ALL SELECT * FROM (SELECT leaf_categ_id, SUM(price) AS sum_price FROM kylin_sales GROUP BY leaf_categ_id UNION ALL SELECT leaf_categ_id, SUM(price) AS sum_price FROM kylin_sales GROUP BY leaf_categ_id) CROSS JOIN (SELECT SUM(price) AS sum_price_2 FROM kylin_sales GROUP BY leaf_categ_id) ORDER BY 1;
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