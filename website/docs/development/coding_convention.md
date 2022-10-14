---
title: Coding and Naming Convention
language: en
sidebar_label: Coding and Naming Convention
pagination_label: Coding and Naming Convention
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/roadmap
pagination_next: development/how_to_contribute
keywords:
    - contribute
    - code review
draft: false
last_update:
    date: 10/14/2022
    author: Tengting Xu
---

Coding convention is very important for teamwork. Not only it keeps code neat and tidy, it saves a lot of work too. Different coding convention (and auto formatter) will cause unnecessary code changes that requires more effort at code review and code merge.

## Setup IDE code formatter

For Java code, we use Eclipse default formatter setting, with one change that to allow long lines.

- For Eclipse developers, no manual setting is required. Code formatter configurations `src/core-common/.settings/org.eclipse.jdt.core.prefs` is on git repo. Your IDE should be auto configured when the projects are imported.

- For intellij IDEA developers, you need to install “Eclipse Code Formatter” and load the Eclipse formatter settings into your IDE manually.

  you have to do a few more steps:

  1. Install “Eclipse Code Formatter” and use “org.eclipse.jdt.core.prefs” and “org.eclipse.jdt.ui.prefs” in src/core-common/.settings to configure “Eclipse Java Formatter config file” and “Import order”.

  ![](images/coding_convention/coding_convention_1.png)

  2. Go to Preference => Code Style => Java, set “Scheme” to Default, and set both “Class count to use import with ‘*’” and “Names count to use static import with ‘*’” to 99.

  ![](images/coding_convention/coding_convention_2.png)

  3. Disable intellij IDEA’s “Optimize imports on the fly”
  
  ![](images/coding_convention/coding_convention_3.png)

  4. Format the code before committing the code.

## Setup IDE license header template

Each source file should include the following Apache License header

```shell
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

The checkstyle plugin will check the header rule when packaging also. The license file locates under `dev-support/checkstyle-apache-header.txt`. To make it easy for developers, please add the header as Copyright Profile and set it as default for Kylin project.

![](images/coding_convention/coding_convention_4.png)

## Code Rules(Important)

1. For JavaScript, XML, and other code, please use space for indent. And as a general rule, keep your code format consistent with existing lines. No other enforcement at the moment.

2. Using `StringUtils`, just use the `StringUtils` of `org.apache.commons.lang3.StringUtils` and `io.kyligence.kap.guava20.shaded.common.base.Strings`.

3. Using the `guava` in the package of `io.kyligence.kap.guava20.shaded.`.

4. Don't use `log4j` directly, please use `slf4j`. Lombok uses the annotation of `@Slf4j` and Factory class `org.slf4j.LoggerFactory`.

5. Add a new test or modified a test.

    1) Please using the `junit5` instead of `junit4`. Example, Using the annotation of `org.junit.jupiter.api.Test` instead of `org.junit.Test`.

    2) A test case which extends from `NLocalFileMetadataTestCase` need to change with annotation `@MetadataInfo` and remove the `extend`. 
    
    > Example: org.apache.kylin.junit.MetadataExtension, org.apache.kylin.metadata.epoch.EpochManagerTest
