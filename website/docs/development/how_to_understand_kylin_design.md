---
title: Overall Design of Kylin 5
language: en
sidebar_label: Overall Design of Kylin 5
pagination_label: Overall Design of Kylin 5
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/how_to_release
pagination_next: development/dev_design/metastore_design
showLastUpdateAuthor: true
showLastUpdateTime: true
keywords:
  - dev-design
draft: false
last_update:
  date: 09/21/2022
  author: Xiaoxiang Yu
---

:::info
Unless more comments, all source code analysis are based on [this code snapshot](https://github.com/apache/kylin/tree/edab8698b6a9770ddc4cd00d9788d718d032b5e8) .
:::

### About Design of Kylin 5.0
1. Metadata Store
   - [x] Metadata Store
   - [ ] Metadata Cache
   - [x] Transaction(CRUD of Metadata)
   - [ ] Epoch, AuditLog etc.
2. Metadata Format/Schema
   - [ ] DataModel, IndexPlan and Dataflow
   - [ ] Index and Layout
   - [ ] Computed Column
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