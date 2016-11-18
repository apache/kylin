---
layout: dev
title:  How to Maintain HBase Branches
categories: development
permalink: /development/howto_hbase_branches.html
---

### Kylin Branches for Different HBase Versions

Because HBase API diverges based on versions and vendors, different code branches have to be maintained for different HBase versions.

The branching design is

- The `master` branch compiles with HBase 0.98, and is also the main branch for development. All bug fixes and new features commits to `master` only.
- The `master-hbase1.x` branch compiles with HBase 1.x. This branch is created by applying one patch on top of `master`. In other word, `master-hbase1.x` = `master` + `a patch to support HBase 1.x`.
- Similarly, there is `master-cdh5.7` = `master-hbase1.x` + `a patch to support CDH 5.7`.
- No code changes should happen on `master-hbase1.x` and `master-cdh5.7` directly (apart from the last commit on the branch that adapts HBase calls).

There is a script helps to keep these branches in sync: `dev-support/sync_hbase_cdh_branches.sh`.




