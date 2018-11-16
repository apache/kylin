---
layout: docs
title:  Upgrade Coprocessor
categories: howto
permalink: /docs/howto/howto_upgrade_coprocessor.html
---

Kylin leverages HBase coprocessor to optimize query performance. After upgrading to a new version of Kylin, the RPC protocol may get changed, so users need to redeploy coprocessor to HTable.

There's a CLI tool to update HBase Coprocessor:

```sh
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI default all
```