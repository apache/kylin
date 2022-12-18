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

### Kylin 5.0.0

- More flexible and enhanced data model
  - Allow adding new dimensions and measures to the existing data model
  - The model adapts to table schema changes while retaining the existing index at the best effort
  - Support last-mile data transformation using Computed Column
  - Support raw query (non-aggregation query) using Table Index
  - Support changing dimension table (SCD2)
- Simplified metadata design
  - Merge DataModel and CubeDesc into new DataModel
  - Add DataFlow for more generic data sequence, e.g. streaming alike data flow
  - New metadata AuditLog for better cache synchronization
- More flexible index management (was cuboid)
  - Add IndexPlan to support flexible index management
  - Add IndexEntity to support different index type
  - Add LayoutEntity to support different storage layouts of the same Index
- Toward a native and vectorized query engine
  - Experiment: Integrate with a native execution engine, leveraging Gluten
  - Support async query
  - Enhance cost-based index optimizer
- More
  - Build engine refactoring and performance optimization
  - New WEB UI based on Vue.js, a brand new front-end framework, to replace AngularJS
  - Smooth modeling process in one canvas

### Kylin 5.1.0

- Support deploying Kylin on K8S with micro-service architecture
