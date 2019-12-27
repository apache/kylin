---
layout: docs31
title:  Use Beeline for Hive
categories: howto
permalink: /docs31/howto/howto_use_beeline.html
---

Beeline(https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients) is recommended by many venders to replace Hive CLI. By default Kylin uses Hive CLI to synchronize Hive tables, create flatten intermediate tables, etc. By simple configuration changes you can set Kylin to use Beeline instead.

Edit $KYLIN_HOME/conf/kylin.properties by:

  1. change kylin.hive.client=cli to kylin.hive.client=beeline
  2. add "kylin.hive.beeline.params", this is where you can specify beeline command parameters. Like username(-n), JDBC URL(-u),etc. There's a sample kylin.hive.beeline.params included in default kylin.properties, however it's commented. You can modify the sample based on your real environment.

