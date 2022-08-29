---
title: Roadmap of Apache Kylin
language: en
sidebar_label: Roadmap of Apache Kylin
pagination_label: Roadmap of Apache Kylin
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/intro
pagination_next: development/how_to_contribute
keywords:
  - roadmap
draft: false
last_update:
  date: 08/24/2022
  author: Xiaoxiang Yu
---

# Roadmap of Apache Kylin

### Kylin 5.0.0-alpha & Kylin 5.0.0

- Re-design metadata schema to support new feature
  - Merge DataModel and CubeDesc into new DataModel
  - Add IndexPlan to support flexible index management
  - Add IndexEntity to support different index type
  - Add LayoutEntity to support different storage layout of same Index
  - Add DataFlow/ComputedColumnDesc etc
- New metadata cache synchronization via AuditLog
- Rewrite model engine and build engine to implement some important new features:
  - Support schema change
  - Add Computed Column to support light-weight ETL
  - Add Table Index to support raw query(non-aggregation query)
  - SCD2
- Rewrite whole WEB UI
  - Use Vue.js, a brand new front-end framework, to replace AngularJS
  - Simplify modeling process into one canvas 
- Integrated with native compute engine preliminarily
- Async query
- Cost-based index optimizer

### Kylin 5.1.0

- Support deploy Kylin on K8S with micro-service architecture
