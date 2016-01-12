---
layout: docs
title:  "Technical Concepts"
categories: gettingstarted
permalink: /docs/gettingstarted/concepts.html
version: v1.2
since: v1.2
---
 
Here are some basic technical concepts used in Apache Kylin, please check them for your reference.
For terminology in domain, please refer to: [Terminology](terminology.md)

## CUBE
* __Table__ - This is definition of hive tables as source of cubes, which must be synced before building cubes.
![](/images/docs/concepts/DataSource.png)

* __Data Model__ - This describes a [STAR SCHEMA](https://en.wikipedia.org/wiki/Star_schema) data model, which defines fact/lookup tables and filter condition.
![](/images/docs/concepts/DataModel.png)

* __Cube Descriptor__ - This describes definition and settings for a cube instance, defining which data model to use, what dimensions and measures to have, how to partition to segments and how to handle auto-merge etc.
![](/images/docs/concepts/CubeDesc.png)

* __Cube Instance__ - This is instance of cube, built from one cube descriptor, and consist of one or more cube segments according partition settings.
![](/images/docs/concepts/CubeInstance.png)

* __Partition__ - User can define a DATE/STRING column as partition column on cube descriptor, to separate one cube into several segments with different date periods.
![](/images/docs/concepts/Partition.png)

* __Cube Segment__ - This is actual carrier of cube data, and maps to a HTable in HBase. One building job creates one new segment for the cube instance. Once data change on specified data period, we can refresh related segments to avoid rebuilding whole cube.
![](/images/docs/concepts/CubeSegment.png)

* __Aggregation Group__ - Each aggregation group is subset of dimensions, and build cuboid with combinations inside. It aims at pruning for optimization.
![](/images/docs/concepts/AggregationGroup.png)

## DIMENSION & MEASURE
* __Mandotary__ - This dimension type is used for cuboid pruning, if a dimension is specified as “mandatory”, then those combinations without such dimension are pruned.
* __Hierarchy__ - This dimension type is used for cuboid pruning, if dimension A,B,C forms a “hierarchy” relation, then only combinations with A, AB or ABC shall be remained. 
* __Derived__ - On lookup tables, some dimensions could be generated from its PK, so there's specific mapping between them and FK from fact table. So those dimensions are DERIVED and don't participate in cuboid generation.
![](/images/docs/concepts/Dimension.png)

* __Count Distinct(HyperLogLog)__ - Immediate COUNT DISTINCT is hard to calculate, a approximate algorithm - [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) is introduced, and keep error rate in a lower level. 
* __Count Distinct(Precise)__ - Precise COUNT DISTINCT will be pre-calculated basing on RoaringBitmap, currently only int or bigint are supported.
* __Top N__ - (Will release in 2.x) For example, with this measure type, user can easily get specified numbers of top sellers/buyers etc. 
![](/images/docs/concepts/Measure.png)

## CUBE ACTIONS
* __BUILD__ - Given an interval of partition column, this action is to build a new cube segment.
* __REFRESH__ - This action will rebuilt cube segment in some partition period, which is used in case of source table increasing.
* __MERGE__ - This action will merge multiple continuous cube segments into single one. This can be automated with auto-merge settings in cube descriptor.
* __PURGE__ - Clear segments under a cube instance. This will only update metadata, and won't delete cube data from HBase.
![](/images/docs/concepts/CubeAction.png)

## JOB STATUS
* __NEW__ - This denotes one job has been just created.
* __PENDING__ - This denotes one job is paused by job scheduler and waiting for resources.
* __RUNNING__ - This denotes one job is running in progress.
* __FINISHED__ - This denotes one job is successfully finished.
* __ERROR__ - This denotes one job is aborted with errors.
* __DISCARDED__ - This denotes one job is cancelled by end users.
![](/images/docs/concepts/Job.png)

## JOB ACTION
* __RESUME__ - Once a job in ERROR status, this action will try to restore it from latest successful point.
* __DISCARD__ - No matter status of a job is, user can end it and release resources with DISCARD action.
![](/images/docs/concepts/JobAction.png)