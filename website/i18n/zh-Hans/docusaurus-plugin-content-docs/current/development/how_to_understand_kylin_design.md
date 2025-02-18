---
title: Kylin 5 整体设计
language: zh-Hans
sidebar_label: Kylin 5 整体设计
pagination_label: Kylin 5 整体设计
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/how_to_subscribe_mailing_list
pagination_next: development/security
showLastUpdateAuthor: true
showLastUpdateTime: true
keywords:
  - dev-design
draft: false
last_update:
  date: 10/21/2024
  author: Xiaoxiang Yu, Jiang Longfei
---

:::info
除非有更多的注释，否则所有的源代码分析都是基于这个[代码快照](https://github.com/apache/kylin/tree/edab8698b6a9770ddc4cd00d9788d718d032b5e8)。
:::

### 关于 Kylin 5.0 的设计
1. Metadata Store
   - [x] Metadata Store
   - [ ] Metadata Cache
   - [x] Transaction(CRUD of Metadata)
   - [ ] Epoch, AuditLog, etc.
2. Metadata Format/Schema
   - [x] DataModel, IndexPlan, and Dataflow
   - [x] Index and Layout
   - [ ] Computed Column
   - [ ] Schema Change
3. Query Engine
   - [ ] How a SQL query was executed in Kylin?
   - [ ] Query Cache
   - [ ] Query Profile
4. Model Engine
   - [ ] Schema Change(Model Change, Table Change)
5. Build Engine
   - [ ] Build AggIndex
   - [ ] Build TableIndex
   - [ ] Build Snapshot
6. Job Engine
   - [ ] JobScheduler
   - [ ] HA(and epoch)