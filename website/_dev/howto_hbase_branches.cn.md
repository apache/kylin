---
layout: dev-cn
title:  如何维护 HBase 分支
categories: development
permalink: /cn/development/howto_hbase_branches.html
---

### Kylin 不同版本的 HBase 分支 

因为 HBase API 基于版本和供应商的不同，因此必须针对不同的 HBase 版本维护不同的代码分支。

分支设计为

- `master` 分支编译的是 HBase 0.98，也是开发的主要分支。 所有错误修复和新功能仅提交给 `master`。
- `master-hbase1.x` 分支编译的是 HBase 1.x。通过在 `master` 上应用一个 patch 来创建此分支。换句话说，`master-hbase1.x` = `master` + `a patch to support HBase 1.x`.
- 同样的，有 `master-cdh5.7` = `master-hbase1.x` + `a patch to support CDH 5.7`。
- 在 `master-hbase1.x` 和 `master-cdh5.7` 上不会直接发生代码更改（除非分支上最后一次提交采用了 HBase 调用)。

有一个脚本有助于保持这些分支同步：`dev-support/sync_hbase_cdh_branches.sh`。




