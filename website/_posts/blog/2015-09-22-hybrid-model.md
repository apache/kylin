---
layout: post-blog
title:  "Hybrid Model in Apache Kylin 1.0"
date:   2015-09-25 16:00:00
author: Shaofeng Shi
categories: blog
---

**Apache Kylin v1.0 introduces a new realization "hybrid model" (also called "dynamic model"); This post introduces the concept and how to define it.**

# Problem

For incoming SQL queries, Kylin picks ONE (and only ONE) realization to serve the query; Before the "hybrid", there is only one type of realization open for user: Cube. That to say, only 1 Cube would be selected to answer a query;

Now let's start with a sample case; Assume user has a Cube called "Cube_V1", it has been built for a couple of months; Now the user wants to add new dimension or metrics to fulfill their business need; So he created a new Cube named "Cube_V2"; 

Due to some reason user wants to keep the data of "Cube_V1", and expects to build "Cube_V2" from the end date of "Cube_V1"; The possible reasons include:

* History source data has been dropped from Hadoop, not possible to build "Cube_V2" from the very beginning;
* The cube is large, rebuilding takes very long time;
* New dimension/metrics is only feasible for the new date, or user feels fine if they were absent for old cube; etc.

For some queries that don't use the new measure and metrics, user hopes both "Cube_V1" and "Cube_V2" can be scanned to get a full result, such as "select count(*)...", "select sum(price)..."; With such a background, the "hybrid model" is introduced in Kylin;

## Hybrid Model

Hybrid model is a new realization which is a composite of one or multiple other realizations (cubes); See the figure below.

![]( /images/blog/hybrid-model.png)

Hybrid doesn't have its real storage; It is just like a virtual database view over tables; It acts as a delegator who delegates the requests to its children realizations.

## How to add a Hybrid model

As there is no UI for creating/editing hybrid model, if have the need, you need manually edit Kylin metadata;

### Step 1: Take a backup of kylin metadata store 

```
export KYLIN_HOME="/path/to/kylin"

$KYLIN_HOME/bin/metastore.sh backup

```

A backup folder will be created, assume it is $KYLIN_HOME/metadata_backup/2015-09-25/
 
### Step 2: Create sub-folder "hybrid" in the metadata folder,

```
mkdir -p $KYLIN_HOME/metadata_backup/2015-09-25/hybrid
```

### Step 3: Create a hybrid json file: 

```
vi $KYLIN_HOME/metadata_backup/2015-09-25/hybrid/my_hybrid.json

```

Input content like this:

```
{
  "uuid": "9iiu8590-64b6-4367-8fb5-7500eb95fd9c",
  "name": "my_hybrid",
  "realizations": [
    {
           "type": "CUBE",
           "realization": "Cube_V1"
    },
    {
            "type": "CUBE",
            "realization": "Cube_V2"
    }
  ]
}

```
Here "Cube_V1" and "Cube_V2" are the cubes that you want to combine.


### Step 4: Add hybrid model to project

Open project json file (for example project "default") with text editor:

```
vi $KYLIN_HOME/metadata_backup/2015-09-25/project/default.json

```

In the "realizations" array, add one entry like:

```
    {
      "name": "my_hybrid",
      "type": "HYBRID",
      "realization": "my_hybrid"
    }
```

### Step 5: Upload the metadata:

```
  $KYLIN_HOME/bin/metastore.sh restore $KYLIN_HOME/metadata_backup/2015-09-25/

```

### Step 6: Reload metadata

Restart Kylin server, or click "Reload metadata" in the "Admin" tab on Kylin web UI to load the changes; Ideally the hybrid will start to work; You can do some verifications.

## FAQ：

**Question 1**: when will hybrid be selected to serve query?
If one of the cube can answer the query, the hybrid which has it as a child will be selected;

**Question 2**: how hybrid to answer the query?
Hybrid will delegate the query to each of its child realization (if it is capable); And then return all the results to query engine; Query engine will aggregate before return to user;

**Question 3**: will hybrid check the data duplication?
No; it depends on you to ensure the cubes in a hybrid don't have date/time range duplication; For example, the "Cube_V1" is ended at 2015-9-20 (including), the "Cube_V2" should start from 2015-9-21 or later;

**Question 4**: will hybrid restrict the children cubes having the same data model?
No; hybrid doesn't check the cube's fact/lookup tables and join conditions at all;

**Question 5**: can hybrid have another hybrid as child?
No; didn't see the need; so far it assumes all children are Cubes;
