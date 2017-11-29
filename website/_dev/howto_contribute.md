---
layout: dev
title:  How to Contribute
categories: development
permalink: /development/howto_contribute.html
---

Apache Kylin is always looking for contributions of not only code, but also usage document, performance report, Q&A etc. All kinds of contributions pave the way towards a Kylin Committer. There is opportunity for everyone, especially for those come from analysis and solution background, due to the lacking of content from user and solution perspective.


## Source Branches
Both code and document are under Git source control. Note the purpose of different branches.

* `master`: Main development branch for new features
* `2.0.x`: Maintenance branch for a certain release
* `document`: Document branch


## Pick an Open Task
There are open tasks waiting to be done, tracked by JIRA. To make it easier to search, there are a few JIRA filters.

* [A list of tasks](https://issues.apache.org/jira/issues/?filter=12339895) managed by Yang Li.
* [A list of tasks](https://issues.apache.org/jira/issues/?filter=12341496) opened by Ted Yu, important small bugs and some are easy fixes.
* Also you can search for tag "newbie" in Kylin JIRA.

Do not forget to discuss in [mailing list](/community/index.html) before working on a big task.


## Making Code Changes
* [Setup dev env](/development/dev_env.html)
* Raise a JIRA, describe the feature/enhancement/bug
* Discuss with others in mailing list or issue comments, make sure the proposed changes fit in with what others are doing and have planned for the project
* Make changes in your fork
	* No strict code style at the moment, but the general rule is keep consistent with existing files. E.g. use 4-space indent for java files.
	* Add unit test for your code change as much as possible.
	* Make sure "mvn clean package" and "mvn test" can get success.
	* Sufficient unit test and integration test is a mandatory part of code change. 
* [Run tests](/development/howto_test.html) to ensure your change is in good quality and does not break anything. If your patch was generated incorrectly or your code does not adhere to the code guidelines, you may be asked to redo some work.
* Generate a patch and attach it to relative JIRA.

## Generate Patch
* Using `submit-patch.py` (recommended) to create patches, upload to jira and optionally create/update reviews on Review Board. Patch name is automatically formatted as (JIRA).(branch name).(patch number).patch to follow Yetus' naming rules. 

```
$ ./dev-support/submit-patch.py -jid KYLIN-xxxxx -b master -srb
```

* Use -h flag for this script to know detailed usage information. Most useful options are:
	* -b BRANCH, --branch BRANCH : Specify base branch for generating the diff. If not specified, tracking branch is used. If there is no tracking branch, error will be thrown.
	* -jid JIRA_ID, --jira-id JIRA_ID : If used, deduces next patch version from attachments in the jira and uploads the new patch. Script will ask for jira username/password for authentication. If not set, patch is named <branch>.patch.
* By default, it'll also create/update review board. To skip that action, use -srb option. It uses 'Issue Links' in the jira to figure out if a review request already exists. If no review request is present, then creates a new one and populates all required fields using jira summary, patch description, etc. Also adds this review’s link to the jira.
* To install required python dependencies, execute `pip install -r dev-support/python-requirements.txt` from the master branch.

* Alternatively, you can also manually generate a patch. Please use `git rebase -i` first, to combine (squash) smaller commits into a single larger one. Then use `git format-patch` command to generate the patch, for a detail guide you can refer to [How to create and apply a patch with Git](https://ariejan.net/2009/10/26/how-to-create-and-apply-a-patch-with-git/)


## Apply Patch
* Committer will review Pull Requests and Patches in JIRA regarding correctness, performance, design, coding style, test coverage
* Discuss and revise if necessary;
* Finally committer merge code into target branch
	* For a git patch, use "git am -s -3 patch-file" command to apply;
	* If it is from a github Pull Request, need add "This closes #<PR NUMBER>" as part of the commit messages. This will allow ASF Git bot to close the PR.
	* Use `git rebase` to ensure the merged result is a streamline of commits.


## Making Document Changes
Check out [How to Write Document](/development/howto_docs.html).

