---
layout: post-blog
title:  New Aggregation Group
date:   2016-02-18 16:30:00
author: Hongbin Ma
categories: blog
---

Full title of this article: `New Aggregation Group Design to Tackle Curse of Dimension Problem (Especially when high cardinality dimensions exist)`

## Abstract

Curse of dimension is an infamous problem for all of the OLAP engines based on pre-calculation. In versions prior to v1.5, Kylin tried to address the problem by some simple techniques, which relieved the problem to some degree. During our open source practices, we found these techniques lack of systematic design thinking, and incapable of addressing lots of common issues. In Kylin v1.5 we redesigned the aggregation group mechanism to make it better server all kinds of cube design scenarios.

##  Introduction

It is a known fact that Kylin speeds up query performance by pre-calculating cubes, which in term contains different combination of all dimensions, a.k.a. cuboids. The problem is that #cuboids grows exponentially with the #dimension. For example, there’re totally 8 possible cuboids for a cube with 3 dimensions, however there are 16 possible cuboids for a cube with 4 dimensions. Even though Kylin is using scalable computation framework (MapReduce) and scalable storage (HBase) to compute and store the cubes, it is still unacceptable if cube size turns up to be times bigger than the original data source.

The solution is to prune unnecessary dimensions. As we previously discussed in http://kylin.apache.org/docs/howto/howto_optimize_cubes.html, it can be approached by two ways:

First, we can remove dimensions those do NOT necessarily have to be dimensions. For example, imagine a date lookup table where keeps cal_dt is the PK column as well as lots of deriving columns like week_begin_dt, month_begin_dt. Even though analysts need week_begin_dt as a dimension, we can prune it as it can always be calculated from dimension cal_dt, this is the “derived” optimization.

Second, some of combinations between dimensions can be pruned. This is the main discuss for this article, and let’s call it “combination pruning”. For example, if a dimension is specified as “mandatory”, then all of the combinations without such dimension can be pruned. If dimension A,B,C forms a “hierarchy” relation, then only combinations with A, AB or ABC shall be remained. Prior to v1.5, Kylin also had an “aggregation group” concept, which also serves for combination pruning. However it is poorly documented and hard to understand (I also found it is difficult to explain). Anyway we’ll skip it as we will re-define what “aggregation group” really is.

During our open source practice we found some significant drawbacks for the original combination pruning techniques. Firstly, these techniques are isolated rather than systematically well designed. Secondly, the original aggregation group is poorly designed and documented that it is hardly used outside eBay. Thirdly, which is the most important one, it’s not expressive enough in terms of describing semantics.

To illustrate the describing semantic issue, let’s imagine a transaction data cube where there is a very high cardinality dimension called buyer_id, as well as other normal dimensions like transaction date cal_dt, buyers’ location city, etc. The analyst might need to get an overview impression by grouping non-buyer_id dimensions, like grouping only cal_dt. The analyst might also need to drill down to check a specific buyer’s behavior by providing a buyer_id filter. Given the fact that buy_id has really high cardinality, once the buyer_id is determined, the related records should be very few (so just use the base cuboid and do some query time aggregation to “aggregate out” the unwanted dimensions is okay). In such cases the expected output of pruning policy should be:

| Cuboid               	| Compute or Skip 	| Reason                                                                                                                 	|
|----------------------	|-----------------	|------------------------------------------------------------------------------------------------------------------------	|
| city                 	| compute         	| Group by location                                                                                                      	|
| cal_dt               	| compute         	| Group by date                                                                                                          	|
| buyer_id             	| skip            	| Group by buyer yield too many results to analyze, buyer_id should be used as a filter and used by visiting base cuboid 	|
| city,cal_dt          	| compute         	| Group by location and date                                                                                             	|
| city,buyer_id        	| skip            	| Group by buyer yield too many results to analyze, buyer_id should be used as a filter and used by visiting base cuboid 	|
| cal_dt,buyer_id      	| skip            	| Group by buyer yield too many results to analyze, buyer_id should be used as a filter and used by visiting base cuboid 	|
| city,cal_dt,buyer_id 	| compute         	| Base cuboid                                                                                                            	|

Unfortunately there is no way to express such pruning settings with the existing semantic tools prior to Kylin v1.5

##  New Aggregation Group Design

In Kylin v1.5 we redesigned the aggregation group mechanism in the jira issue https://issues.apache.org/jira/browse/KYLIN-242. The issue was named “Kylin Cuboid Whitelist” because the new design even enables cube designer to specify expected cuboids by keeping a whitelist, imagine how expressive it can be!

In the new design, aggregation group (abbr. AGG) is defined as a cluster of cuboids that subject to shared rules. Cube designer can define one or more AGG for a cube, and the union of all AGGs’ contributed cuboids consists of the valid combination for a cube. Notice a cuboid is allowed to appear in multiple AGGs, and it will only be computed once during cube building.

If you look into the internal of AGG (https://github.com/apache/kylin/blob/kylin-1.5.0/core-cube/src/main/java/org/apache/kylin/cube/model/AggregationGroup.java) there’re two important properties defined: `@JsonProperty("includes")` and `@JsonProperty("select_rule")`.

`@JsonProperty("includes")`
This property is for specifying which dimensions are included in the AGG. The value of the property must be a subset of the complete dimensions. Keep the proper minimal by including only necessary dimensions.

`@JsonProperty("select_rule")`
Select rules are the rules that all valid cuboids in the AGG will subject to. Here cube designers can define multiple rules to apply on the included dimensions, currently there’re three types of rule:

* Hierarchy rules, described above
* Mandatory rule, described above
* Joint rules. This is a newly introduced rule. If two or more dimensions are “joint”, then any valid cuboid will either contain none of these dimensions, or contain them all. In other words, these dimensions will always be “together”. This is useful when the cube designer is sure some of the dimensions will always be queried together. It is also a nuclear weapon for combination pruning on less-likely-to-use dimensions. Suppose you have 20 dimensions, the first 10 dimensions are frequently used and the latter 10 are less likely to be used. By joining the latter 10 dimensions as “joint”, you’re effectively reducing cuboid numbers from 220 to 211. Actually this is pretty much what the old “aggregation group” mechanism was for. If you’re using it prior Kylin v1.5, our metadata upgrade tool will automatically translate it to joint semantics.
By flexibly using the new aggregation group you can in theory control whatever cuboid to compute/skip. This could significant reduce the computation and storage overhead, especially when the cube is serving for a fixed dashboard, which will reproduce SQL queries that only require some specific cuboids. In extreme cases you can configure each AGG contain only one cuboid, and a handful of AGGs will consists of the cuboid whitelist that you’ll need.

Kylin’s cuboid computation scheduler will arrange all the valid cuboids’ computation order based on AGG definition. You don’t need to care about how it’s implemented, because every cuboid will just got computed and computed only once. The only thing you need to keep in mind is: don’t abuse AGG. Leverage AGG’s select rules as much as possible, and avoid introducing a lot of “single cuboid AGG” unless it’s really necessary. Too many AGG is a burden for cuboid computation scheduler, as well as the query engine.

##  Buyer_id issue revisited

Now that we have got the new AGG tool, the buyer_id issue can be revisited. What we need to do is to define two AGGs for the cube:

* AGG1 includes: [cal_dt, city, buyer_id] select_rules:{joint:[cal_dt,city,buyer_id]}
* AGG2 includes: [cal_dt,city] select rules:{}

---

The first AGG will contribute the base cuboid only, and the second AGG will contribute all the cuboids without buyer_id.

##  Start using it

The new aggregation group mechanism should be available in Kylin v1.5. Up to today (2016.2.18) Kylin has not released v1.5 version yet. Use it at your own risk by compiling the latest master code branch.

For legacy users you will need to upgrade your metadata store from Kylin v1.4 to Kylin v1.5. Cube rebuild is not required if you’re upgrading from Kylin v1.4.