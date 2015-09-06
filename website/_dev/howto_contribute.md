---
layout: dev
title:  How to Contribute
categories: development
permalink: /development/howto_contribute.html
---

## Current branches

Here are the major branches:

* 0.7-staging: dev branch for 0.7 versions, this branch spawns releases like 0.7.1, 0.7.2, the next release would be 0.7.3.
* 0.8: dev branch for 0.8 versions, 0.8 is like the next generation Kylin (with streaming, spark support), it has fundamental difference with 0.7 version, which means any changes on 0.7 will not be merged to 0.8 anymore. So if your patch affects both of the branch, you should make patches for both branch.
* master: always point to the latest stable release(stable, but not up to date)

## Making Changes
* Raise an issue on JIRA, describe the feature/enhancement/bug
* Discuss with others in mailing list or issue comments, make sure the proposed changes fit in with what others are doing and have planned for the project
* Make changes in your fork
* Write unit test if no existing cover your change
* Generate patch and attach it to relative JIRA; Please use "git format-patch" command to generate the patch, for a detail guide you can refer to [How to create and apply a patch with Git](https://ariejan.net/2009/10/26/how-to-create-and-apply-a-patch-with-git/)


## Apply Patch
* Committer will review in terms of correctness, performance, design, coding style, test coverage
* Discuss and revise if necessary
* Finally committer merge code into main branch


