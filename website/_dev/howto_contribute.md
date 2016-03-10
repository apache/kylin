---
layout: dev
title:  How to Contribute
categories: development
permalink: /development/howto_contribute.html
---

## Current branches
* `master`: Main development branch for new features
* `1.3.x`: Maintenance branch for a certain release
* `document`: Document branch

## Making Changes
* [Setup dev env](/development/dev_env.html)
* Raise a JIRA, describe the feature/enhancement/bug
* Discuss with others in mailing list or issue comments, make sure the proposed changes fit in with what others are doing and have planned for the project
* Make changes in your fork
	* No strict code style at the moment, but the general rule is keep consistent with existing files. E.g. use 4-space indent for java files.
	* Sufficient unit test and integration test is a mandatory part of code change.
* [Run tests](/development/howto_test.html) to ensure your change is in good quality and does not break anything
* Generate patch and attach it to relative JIRA. Please use `git format-patch` command to generate the patch, for a detail guide you can refer to [How to create and apply a patch with Git](https://ariejan.net/2009/10/26/how-to-create-and-apply-a-patch-with-git/)


## Apply Patch
* Committer will review in terms of correctness, performance, design, coding style, test coverage
* Discuss and revise if necessary
* Finally committer merge code into target branch
	* We use `git rebase` to ensure the merged result is a streamline of commits.
