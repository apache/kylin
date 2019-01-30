---
layout: docs
title:  SQL Function
categories: tutorial
permalink: /docs/tutorial/kylin_function.html
---
   
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
