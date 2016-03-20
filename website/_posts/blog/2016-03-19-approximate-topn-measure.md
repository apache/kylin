---
layout: post-blog
title:  Approximate Top-N support in Kylin
date:   2016-03-19 16:30:00
author: Shaofeng Shi
categories: blog
---


## Background

Find the Top-N (or Top-K) entities from a dataset is a common scenario and requirement in data minding; We often see the reports or news like "Top 100 companies in the world", "Most popular 20 electronics sold on eBay", etc. Exploring and analysising the top entities can always find some high value information.

Within the era of big data, this need is much stronger than ever before, as both the raw dataset and the number of entities can be vast; Without certain pre-calculation, get the Top-K entities among a distributed big dataset may take a long time, makes the ad-hoc query inefficient. 

In v1.5.0, Apache Kylin introduces the "Top-N" measure, aiming to pre-calculate the top entities during the cube build phase; in the query phase,  Kylin can quickly fetch and return the top records. The performance would be much better than a cube without "Top-N", giving the analyst more power to inspect data. 

Please note, this "Top-N" measure is an approximate realization, to use it well you need have a good understanding with the algorithm as well as the data distribution.

##  Top-N query

Let's start with the sample table that shipped in Kylin binary package. If you haven't run that, follow this tutorial to create it: [Quick Start with Sample Cube](https://kylin.apache.org/docs15/tutorial/kylin_sample.html). 

The sample fact table "default.kylin\_sales" mock the transactions on an online marketplace. It has a couple of dimension and measure columns. To be simple, here we only use four: "PART\_DT", "LSTG\_SITE\_ID", "SELLER\_ID" and "PRICE". Bellow table is the concept of these columns, with a rough cardinality, the "SELLER\_ID" is a high cardinality column. 


| Column  | Description | Cardinality|
|:------------- |:---------------:| :-------------:|
| PART\_DT     | Transaction Date |          730: two years |
| LSTG\_SITE\_ID      | Site ID, 0 represents 'US'        |           50 |
| SELLER\_ID | Seller ID        |            About one million |
| PRICE | Sold amount       |        -   |

Very often this online marketplace company need to identify the top sellers  (say top 100) in a given time period in some countries. The query looks like:

```
SELECT SELLER_ID, SUM(PRICE) FROM KYLIN_SALES
 WHERE 
	PART_DT >= date'2016-02-18' AND PART_DT < date'2016-03-18' 
		AND LSTG_SITE_ID in (0) 
	group by SELLER_ID 
	order by SUM(PRICE) DESC limit 100;
```

## Without Top-N pre-calculation

Before Kylin v1.5.0, all the "group by" columns need be as dimension, we come of a design that use PART\_DT, LSTG\_SITE\_ID and SELLER\_ID as dimensions, and define SUM(PRICE) as the measure. After build, the base cubiod of the cube will be like:

| Rowkey of base cuboid  | SUM(PRICE) |
|:------------- |:---------------:| 
| 20140318\_00\_seller0000001    | xx.xx | 
| 20140318\_00\_seller0000002    | xx.xx |  
|...|...|
| 20140318\_00\_seller0999999    | xx.xx | 
| 20140318\_01\_seller0999999    | xx.xx |   
|...|...|
|...|...|      
| 20160318\_49\_seller0999999    | xx.xx |   

Assume these dimensions are independent. The number of rows in base cuboid is 730\*50\*1million = 36.5 billion. Other cuboids which include "SELLER\_ID" will also has millions of rows. At this moment you may notice that the cube expansion rate is high, the situation would be worse if there are more dimensions or the cardinality is higher. But the real challenge is not here. 

Soon you will find the Top-N query couldn't work, or took an unacceptable long time. Assume you want the top sellers in past 30 days in US, it need read 30 million rows from storage, aggregate and sort, finally return the top 100 ones. 

Now we see, due to no pre-calculation, although the final result set is small, the memory footprint and I/Os in between is heavy.

## With Top-N pre-calculation

With the Top-N measure, Kylin will pre-calculate the top entities for each dimension combination duing the cube build, saving the result (both entity ID and measure value) as a column in storage. The entity ID ("SELLER\_ID" in this case) now can be moved from dimension to the measure, which doesn't participate in the rowkey. For the sample scenario described above, the newly designed cube will have 2 dimensions (PART\_DT, LSTG\_SITE\_ID), and 1 Top\-N measure. 

| Rowkey of base cuboid  | Top\-N measure|
|:------------- |:---------------:| 
| 20140318\_00    | seller0010091:xx.xx, seller0005002:xx.xx, ..., seller0001789:xx.xx | 
| 20140318\_01    | seller0032036:xx.xx, seller0010091:xx.xx, ..., seller000699:xx.xx | 
|...|...|
| 20160318\_49   | seller0061016:xx.xx, seller0665091:xx.xx, ..., seller000699:xx.xx | 

The base cuboid will have 730 \* 50 = 36.5 k rows now. In the measure cell, the Top certain records be stored in a container in descending order, those tail entities have been filtered out.

For the same query, "Top sellers in past 30 days in US" now only need read 30 rows from storage. The measure object, also called as counter containers will be further aggregated/merged at the storage side, finally only one container is returned to Kylin. Kylin extract the "SELLER\_ID" and "SUM(PRICE)" from it before returns to client. The cost is much lighter than before, the performance gets highly improved.

## Algorithm

Kylin's Top-N implementation referred to [stream-lib](https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/StreamSummary.java), which is based on the Space-Saving algorithm and the Stream-Summary data structure as described in <i>[1]Efficient Computation of Frequent and Top-k Elements in Data Streams</i> by Metwally, Agrawal, and Abbadi. 

A couple of modifications are made to let it better fit with Kylin:

* Use double as the counter data type;
* Simplfy the data strucutre, using one linked list for all entries;
* Use a more compact serializer;

Besides, in order to run SpaceSaving in parallel on Hadoop, we make it mergable with the algorithm introduced in <i>[2] A parallel space saving algorithm for frequent items and the Hurwitz zeta distribution</i>.


## Accuracy

Although the experiments in paper [1] has proved SpaceSaving's efficiency and accuracy for realistic Zipfian data, it doesn't ensure 100% correctness for all cases. SpaceSaving uses a fixed space to put the most frequent candidates; when the size exceeds the space, the tail elements will be truncated, causing data loss. The parallel algorithm will merge multiple SpaceSavings into one, at that moment for the elements appeared in one but not in the other it had some assumptions, this will also cause some data loss. Finally, the result from Top-N measure may have minor difference with the real result.

A couple of factors can affect the accuracy:

* Zipfian distribution

Many rankings in the world follows the **[3] Zipfian distribution**, such as the population ranks of cities in various countries, corporation sizes, income rankings, etc. But the exponent of the distribution varies in different scenarios, this will affect the correctness of the result. The higher the exponent is (the distribution is more sharp), the more accurate answer will get. When using SpaceSaving, you'd better have an calculation on your data distribution.

* Space in SpaceSaving

As mentioned above, SpaceSaving use a small space to put the most frequent elements. Giving more space it will provide more accurate answer. For example, to calculate Top N elements, using 100 \* N space would provide more accurate answer than 50 \* N space. But more space will take more CPU, memory and storage, this need be balanced.

* Element cardinality

Element cardinality is also a factor to consider. Calculating Top 100 among 10 thousands is easiser than among 10 million.

## Statistics

We designed a test case to calculate the top 100 elements using the parallel SpaceSaving among a data set; The element's occurancy follows the Zipfian distribution, adjust the Zipfian exponent, space, and cardinality time to times, compare the result with the accurate result to collect the statistics, we get a rough accuracy report in below. 

The first column is the element cardinality, means among how many elements to identify the top 100 elements; The other three columns represent how much space using in the algorithm: 20X means using 2,000, 50X means use 5,000. Each cell of the table shows how many records are exactly matched with the real result. The calculation is executed in parallel with 10 threads. 

### Test 1. Calculate top-100 in 1 million records, zif exponent = 0.5

| Element cardinality| 20X space| 	50X space| 100X space|
|-------------: |:---------------:|:---------------:|:---------------:| | 10,000	| 100%	| 100%	| 100% ||20,000	|100%	|100%	|100% ||100,000	|70%	|100%	|100% ||1,000,000|	8%	|45%	|98% |

Test 1: More space can get better accuracy.

### Test 2. Calculate top-100 in 100 million records, zif exponent = 0.5

| Element cardinality	| 20X space| 	50X space| 100X space|
|-------------: |:---------------:|:---------------:|:---------------:| | 10,000	| 100%	| 100%	| 100% ||20,000	|100%	|100%	|100% ||100,000	|60%	|100%	|100% ||1,000,000|	8%	|56%	|96% |

Test 2: The data size doesn't impact much.

### Test 3. Calculate top-100 in 1 million records, zif exponent = 0.6

| Element cardinality	| 20X space| 	50X space| 100X space|
|-------------: |:---------------:|:---------------:|:---------------:| | 10,000	| 100%	| 100%	| 100% ||20,000	|100%	|100%	|100% ||100,000	|94%	|100%	|100% ||1,000,000|	31%	|93%	|100% |

Test 3: more sharp the elements distribute, the better answer it prvoides

### Test 4. Calculate top-100 in 1 million records, zif exponent = 0.7

| Element cardinality	| 20X space| 	50X space| 100X space|
|-------------: |:---------------:|:---------------:|:---------------:| | 10,000	| 100%	| 100%	| 100% ||20,000	|100%	|100%	|100% ||100,000	|100%	|100%	|100% ||1,000,000|	62%	|100%	|100% |

Test 4: same conclusion as test 3.

These statistics matches with what we expected above. It just gives us a rough estimation on the result correctness. To use this feature well in Kylin, you need know about all these variables, and do some pilots before publish it to the analysts. 


##Futher works

This feature in v1.5.0 is a basic version, which may solve 80% cases; While it has some limitations or hard-codings that deserve your attention:

* use SUM() as the default aggregation function;
* sort in descending order always;
* use 50X space always;
* use dictionary encoding for the literal column;
* the UI only allow selecting top 10, 100 and 1000;

Whether or not to support more aggregations/sortings/encodings are totally based on user need. If you have any comment or suggestion, please subscribe and then drop email to our dev mailing list <dev@kylin.apache.org>, thanks for your feedbak.
 
##References

[1] [Efficient Computation of Frequent and Top-k Elements in Data Streams](https://dl.acm.org/citation.cfm?id=2131596)

[2] [A parallel space saving algorithm for frequent items
and the Hurwitz zeta distribution](http://arxiv.org/pdf/1401.0702.pdf)

[3] [Zipfian law on wikipedia](https://en.wikipedia.org/wiki/Zipf%27s_law)
