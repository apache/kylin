---
sidebar_position: 1
---

# Roadmap

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
- Rewrite whole WEB UI
  - Use Vue.js, a brand new front-end framework, to replace AngularJS
  - Simplify modeling process into one canvas 
- Integrated with native compute engine preliminarily
- SCD2, async query, cost-based index optimizer etc

### Kylin 5.1.0

- Support deploy Kylin on K8S with micro-service architecture
