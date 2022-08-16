---
title: Update Session Table Tool
language: en
sidebar_label: Update Session Table Tool
pagination_label: Update Session Table Tool
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - update session table tool
draft: false
last_update:
    date: 08/12/2022
---

When configured `kylin.web.session.secure-random-create-enabled=true` or `kylin.web.session.jdbc-encode-enabled=true`, the session table needs to be upgraded, otherwise the user cannot log in.

**How to Use**

- Use tools to update
  
    ```bash
      $KYLIN_HOME/bin/kylin.sh org.apache.kylin.tool.upgrade.UpdateSessionTableCLI
    ```
    
> Note: During the upgrade process, the update may fail due to permission reasons. At this time, the operation and maintenance personnel need to manually execute the statement to update the session table.

### Sql

**Use PostgreSQL as Metastore**

```sql
ALTER TABLE spring_session ALTER COLUMN SESSION_ID TYPE VARCHAR(180) , ALTER COLUMN SESSION_ID SET NOT NULL;

ALTER TABLE spring_session_ATTRIBUTES ALTER COLUMN SESSION_ID TYPE VARCHAR(180) , ALTER COLUMN SESSION_ID SET NOT NULL;
```

**Use MySQL as Metastore**

```sql
ALTER TABLE spring_session MODIFY COLUMN SESSION_ID VARCHAR(180) NOT NULL;

ALTER TABLE spring_session_ATTRIBUTES MODIFY COLUMN SESSION_ID VARCHAR(180) NOT NULL;
```

