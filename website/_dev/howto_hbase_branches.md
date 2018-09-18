---
layout: dev
title:  How to Maintain Hadoop/HBase Branches
categories: development
permalink: /development/howto_hbase_branches.html
---

### Kylin Branches for Different Hadoop/HBase Versions

Because HBase API diverges based on versions and vendors, different code branches have to be maintained for different HBase versions.

The branching design is

- The `master` branch compiles with HBase 1.1, and is also the main branch for development. All bug fixes and new features commits to `master` only.
- The `master-hadoop3.1` branch compiles with Hadoop 3.1 and HBase 1.x. This branch is created by applying several patches on top of `master`. In other word, `master-hadoop3.1` = `master` + `patches to support Hadoop 3 and HBase 2.x`.
- The `master-hbase0.98` is deprecated;
- There are several release maintenance branches like `2.5.x`, `2.4.x`. If you have a PR or patch, please let reviewer knows which branch it need be applied. The reviewer should cherry-pick the patch to a specific branch after be merged into master.
- No code changes should happen on `master-hadoop3.1`, `master-hbase0.98` directly (apart from the last commit on the branch that adapts HBase calls).

There is a script helps to keep these branches in sync: `dev-support/sync_hbase_cdh_branches.sh`.




