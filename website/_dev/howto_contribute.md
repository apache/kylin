---
layout: dev
title:  How to Contribute
categories: development
permalink: /development/howto_contribute.html
---

## Current branches

Here are the major branches:

* `1.x-staging`: Dev branch for 1,x versions, this branch spawns releases like 1.0, 1.1 etc. This is where new patches go to.
* `2.x-staging`: Dev branch for 2.x versions, 2.x is the next generation Kylin (with streaming, spark support), it has fundamental difference with 1.x version, which means any changes on 1.x cannot merge to 2.x anymore. So if your patch affects both branches, you should make patches for both branches.
* `master`: always point to the latest stable release (stable, but not up to date)

## Making Changes
* Raise an issue on JIRA, describe the feature/enhancement/bug
* Discuss with others in mailing list or issue comments, make sure the proposed changes fit in with what others are doing and have planned for the project
* Make changes in your fork
	* No strict code style at the moment, but the general rule is keep consistent with existing files. E.g. use 4-space indent for java files.
* Write unit test if no existing cover your change
* Generate patch and attach it to relative JIRA; Please use `git format-patch` command to generate the patch, for a detail guide you can refer to [How to create and apply a patch with Git](https://ariejan.net/2009/10/26/how-to-create-and-apply-a-patch-with-git/)


## Apply Patch
* Committer will review in terms of correctness, performance, design, coding style, test coverage
* Discuss and revise if necessary
* Finally committer merge code into target branch

