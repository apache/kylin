---
layout: dev
title:  Coding and Naming Convention
categories: development
permalink: /development/coding_naming_convention.html
---

## Coding Convention

Coding convention is very important for teamwork. Not only it keeps code neat and tidy, it saves a lot of work too. Different coding convention (and auto formatter) will cause unnecessary code changes that requires more effort at code review and code merge.

For Java code, we use Eclipse default formatter setting, with one change that to allow long lines.

- For Eclipse developers, no manual setting is required. Code formatter configurations `.settings/org.eclipse.jdt.core.prefs` is on git repo. Your IDE should be auto configured when the projects are imported.
- For intellij IDEA developers, you need to install "Eclipse Code Formatter" and load the Eclipse formatter settings into your IDE manually. See [Setup Development Env](dev_env.html) for details.
- We have *checkstyle plugin* enabled in maven to enforce some convention checks.

For JavaScript, XML, and other code, please use space for indent. And as a general rule, keep your code format consistent with existing lines. No other enforcement at the moment.



## Configuration Naming Convention

For Kylin configuration names (those in `kylin.properties`)

- The convention is `dot.separated.namespace.config-name-separated-by-dash`, all chars in lower case.
- Rationale: The dot separated prefix is for namespace, like java packages. The last level is like class name, but in lower case and separated by dash. The result is consistent with common hadoop config names, i.e. `dfs.namenode.servicerpc-bind-host`.
- Good examples: `kylin.metadata.url`; `kylin.snapshot.max-mb`
- Bad exmaples: `kylin.cube.customEncodingFactories`, should be `kylin.cube.custom-encoding-factories`
- Namespaces (or packages) of the names should map from the Java project and package where the configuration is used. Below is a list of current namespaces.
  - kylin.env
  - kylin.metadata
  - kylin.snapshot
  - kylin.dictionary
  - kylin.cube
  - kylin.job
  - kylin.engine
  - kylin.engine.mr
  - kylin.engine.spark
  - kylin.source
  - kylin.source.hive
  - kylin.source.kafka
  - kylin.storage
  - kylin.storage.hbase
  - kylin.query
  - kylin.security
  - kylin.server
  - kylin.web



## Configuration File Naming Convention

For configuration files like logging config, spring config, mapreduce job config etc.

- The convention is `words-separated-by-dash.ext`, all chars in lower case.
- Rationale: Be consistent with hadoop config file names, i.e. hdfs-site.xml
- Good example: `kylin-server-log4j.properties`
- Bad example: `kylin_hive_conf.xml`, should be `kylin-hive-conf.xml`


