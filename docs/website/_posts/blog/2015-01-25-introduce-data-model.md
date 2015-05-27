---
layout: post
title:  "Introduce Data Model of Cube Designer"
date:   2015-01-25 22:28:00
author: Luke Han
categories: blog
---

### Background
In previous version (before v0.6.4), Kylin introduced a GUI tool called Cube Designer for user (we called this role as __Cube Modeler__) to architect OLAP Cube with dimensions, measures and other settings. It works well for most of the features but still not user friendly yet: 

1. A user has to add dimension one by one, considering there are 20+ even 50+ dimensions, the entire process is really boring. 
2. Each dimension requires define join condition between fact table and lookup table which even already be defined in previous dimensions many times.
3. Less validation check, especially for Hierarchy and Derived dimension, there are many exceptions in further steps which blocked many people to save the cube definition without any idea about the issue.
4. Save/Next buttons are confusing user to click which one for real next step or just save current dimension settings

### Data Model of Cube Designer
With the feedback from our internal users and external community, we have came up one idea and would like to introduce a new concept (widely known in Data Warehouse and Business Intelligence domain): Data Model: a data model organises data elements and standardises how the data elements relate to one another.[Wikipedia](http://en.wikipedia.org/wiki/Data_model). In Kylin, it using [Star Schema](http://en.wikipedia.org/wiki/Star_schema) as Data Model, which is the simplest style of data warehouse schema. The star schema consists of a few "fact tables" (possibly only one, justifying the name) referencing any number of "dimension tables". It actually already there behind dimensions and measures and now just come to first step to define the relationship between different tables before create each dimension. 
Now (after v0.6.4), to create a cube will follow below steps:

1. Define data model first: pick up one fact table and then add other lookup tables (with their join conditions). The data mode must be presents as Star Schema.
2. Then add dimensions, since all join conditions already presented in data model, each dimension could be more easy to create, just need to know what's kind of type: normal, hierarchy and derived (will have another blog to introduce them). There's also one helper called _Auto Generator_ to help generate many dimensions within simple clicks.
3. Then define measures and others as previous cube designer did

### Benefits
1. A data model is very easy to communicate between different roles and teams. Most of cases it just mapping to real database table relationship, like from Hive tables
2. More easy to create dimensions and measures based on the data model
3. Friendly error message with enhanced validation check when save cube


### What's Next
After this refactor, Kylin is now be able to introduce more powerful features, the major idea is to using different storages to serve same data model:

* __Logical Model__: A Data Model presents logical data structure like Star Schema beyond data tables with more business meaning
* __Physical Model__: define how the underlying data to be stored in persistent system, like HBase. There are already two of them: MOLAP (current Kylin version) and InvertedIndex (coming with 0.7.x release). And it also easy to extend to support others without change the Logical Model.
* A new GUI of Cube Designer to support above is on the way.




