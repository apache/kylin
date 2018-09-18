---
layout: dev-cn
title:  如何维护 HBase 分支
categories: development
permalink: /cn/development/howto_hbase_branches.html
---

### Kylin 不同版本的 HBase 分支 

因为 HBase API 基于版本和供应商的不同，因此必须针对不同的 HBase 版本维护不同的代码分支。

分支设计为

- `master` 分支编译的是 HBase 1.1，也是开发的主要分支。 所有错误修复和新功能仅提交给 `master`。
- `master-hadoop3.1` 分支编译的是 Hadoop 3.1 + HBase 2.x。通过在 `master` 上应用若干个 patch 来创建此分支。换句话说，`master-hadoop3.1` = `master` + `patches to support HBase 2.x`.
- The `master-hbase0.98` 已经弃之不用，0.98用户建议升级HBase;
- 另外有若干个Kylin版本维护分支，如2.5.x, 2.4.x 等；如果你提了一个patch或Pull request, 请告知 reviewer 哪几个版本需要此patch， reviewer 会把 patch 合并到除master以外的其它分支；
- 在 `master-hadoop3.1` 上不会直接发生代码更改（除非分支上最后一次提交采用了 HBase 调用)。

有一个脚本有助于保持这些分支同步：`dev-support/sync_hbase_cdh_branches.sh`。




