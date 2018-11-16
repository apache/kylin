---
layout: docs-cn
title:  升级协处理器
categories: howto
permalink: /cn/docs/howto/howto_upgrade_coprocessor.html
---


Kylin 利用 HBase 协处理器来优化查询性能。升级到新版本后，RPC协议可能会更改，因此用户需要将协处理器重新部署到 HTable。

Kylin 提供了一个 CLI 工具来更新 HBase 协处理器：

```sh
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI default all
```