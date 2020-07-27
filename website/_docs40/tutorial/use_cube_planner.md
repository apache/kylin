---
layout: docs40
title:  Use Cube Planner
categories: tutorial
permalink: /docs40/tutorial/use_cube_planner.html
---

# Cube Planner

## What is Cube Planner

OLAP solution trades off online query speed with offline Cube build cost (compute resource to build Cube and storage resource to save the Cube data). Resource efficiency is the most important competency of OLAP engine. To be resource efficient, It is critical to just pre-build the most valuable cuboids.

Cube Planner makes Apache Kylin to be more resource efficient. It intelligently build a partial Cube to minimize the cost of building a Cube and at the same time maximize the benefit of serving end user queries, then learn patterns from queries at runtime and dynamically recommend cuboids accordingly. 

![CubePlanner](/images/CubePlanner/CubePlanner.png)

Read more at [eBay tech blog](https://www.ebayinc.com/stories/blogs/tech/cube-planner-build-an-apache-kylin-olap-cube-efficiently-and-intelligently/)

## Prerequisites

To enable Dashboard on WebUI, you need to set `kylin.cube.cubeplanner.enabled=true` and other properties in `kylin.properties`

{% highlight Groff markup %}
kylin.cube.cubeplanner.enabled=true
kylin.server.query-metrics2-enabled=true
kylin.metrics.reporter-query-enabled=true
kylin.metrics.reporter-job-enabled=true
kylin.metrics.monitor-enabled=true
{% endhighlight %}

## How to use it

*Note: Cube planner is divided into two phase. Phase 1 can recommend cuboid list based on estimated cuboid size before building the Cube, while phase 2 recommends cuboid list for existing Cube according to query statistics. Cube should be online on production for a while (like 3 months) before optimizing it. So that Kylin platform collects enough real queries from end user and use them to optimize the Cube.*  

Cube planner phase 1 is supported in kylin 4.0, please check document: [How to use Cube Planner in Kylin4.0](https://cwiki.apache.org/confluence/display/KYLIN/How+to+use+Cube+Planner+in+Kylin+4)

For cube planner phase 2, kylin 4.0 only partially supports it. Please check document: [How to update cuboid list for a cube in Kylin4.0](https://cwiki.apache.org/confluence/display/KYLIN/How+to+update+cuboid+list+for+a+cube)

