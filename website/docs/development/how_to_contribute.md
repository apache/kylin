---
title: How to contribute
language: en
sidebar_label: How to contribute
pagination_label: How to contribute
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/roadmap
pagination_next: development/how_to_write_doc
keywords:
    - contribute
    - code review
draft: false
last_update:
    date: 08/29/2022
    author: Tengting Xu, Xiaoxiang Yu
---

Apache Kylin is always looking for all kinds of contributions, including not only code, but also documents, testings, [performance reports](https://cwiki.apache.org/confluence/display/KYLIN/Performance+Benchmark+Report+of+Kylin+4.0.0+vs+Kylin3.1.2+on+Hadoop), 
release management, team coordination, success reference, etc. All of them pave the way towards a [Apache Committer](https://www.apache.org/foundation/how-it-works.html#committers). 

Especially, we appreciate contents from a business and analysis perspective, like analysis best practices and success production scenarios. Technology is only useful when it has an impact to the real world. These contents are greatly helpful to users who are new to the community, however is often overlooked by technical developers.

:::info User story wanted
Please refer to [how to write document](./how_to_write_doc) if you want to contribute business or analysis content. Highly appreciated~
:::


### <span id="branch_table">Source Branches</span>
Both code and document are under Git source control. Note the purpose of different branches.

| Branch            | Category           |                 Comment                | 
|:------------------|--------------------|:---------------------------------------|
| **kylin5**        | Development branch | **Active** development branch for v5.x |
| **doc5.0**        | Document branch    | Document branch for v5.x               |
| **main**          | Maintenance branch | Maintenance branch for v4.x            |
| **kylin3**        | Maintenance branch | Maintenance branch for v3.x     |
| **document**      | Document branch    | Document branch for v4.x and before    |

-----

## Guide for Contributor 

:::note How we collaborate?
Like all ASF projects, Apache Kylin follows [the apache way](https://www.apache.org/foundation/how-it-works) for distributed collaboration around the world.
:::

For new comers, please read about [the project management](https://www.apache.org/foundation/how-it-works.html#management) and [the different roles](https://www.apache.org/foundation/how-it-works.html#roles), to get a quick understanding on the basics.

### Overview of code contribution

The overview is followed by step-by-step instructions.

1. Setup a development environment
2. Pick or create a new JIRA task
3. [Community over code!](http://theapacheway.com/community-over-code/) Discuss your plan with others
4. Make code changes and create a pull request
5. Do code review with a reviewer
6. Make related document changes following [how to write document](./how_to_write_doc).

### Step 1. Setup a development environment
1. Visit the https://github.com/apache/kylin and click the **Fork** button.
   ![](images/fork_github_repo.png)
2. Clone the fork repo to local
   ```shell
   cd /path/you/prefer
   git clone https://github.com/<YourUserName>/kylin.git 
   ```
3. Create a new development branch

   The **base_branch** is determined by which version of Kylin you want to work on and [branches table](#branch_table).

   ```shell
   ## check out to base branch you want to work on
   git checkout <base_branch>
   
   ## create a development branch based on base branch
   git checkout -b <development_branch>
   ```
   
   For example, if I want to fix some issue for Kylin 5.0.0-alpha, you could execute following command:
   ```shell
   git checkout kylin5
   git checkout -b fix_some_issue
   ```

   For better understanding of Git operations, please check [Contributing to a Project](https://www.git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project).

4. Read more about [how to setup an IDE](./how_to_debug_kylin_in_ide).


### Step 2. Pick or create a new JIRA task

Like other ASF projects, we use [JIRA](http://issues.apache.org/jira/browse/KYLIN) to track tasks for kylin development.

:::caution report a possible vulnerability privately
For a possible vulnerability, do not use JIRA as making the info public may harm existing users immediately. Please follow the guide to [report possible vulnerability](https://apache.org/security/committers.html#report) in a private manner.
:::

If you want to create a new JIRA for bug or feature, remember to provide enough information for the community:

* A precise and compact **Summary**
  - e.g. _Failed to read big resource /dict/xxxx at 'Build Dimension Dictionary' Step_
* A correct **Type** of issue
  - _New Feature_ , if you want to develop a new function by yourself
  - _Improvement_ , if you find a way to improve an existent function
  - _Bug_ , if you find something not working as expected
  - _Wish_ , if you want a new function to be provided by others
* **Affected version**: which version of Kylin this issue is about
* A detailed **Description**, which may include:
  - the environment of this problem occurred, OS/Hadoop/Spark versions ...
  - the steps to reproduce the problem
  - the [thread stack](https://issues.apache.org/jira/secure/attachment/13048219/image-2022-08-17-13-17-40-751.png), exception stacktrace or log files (as attachment)
  - the metadata of the model or cube (as attachment)
  - the root cause analysis if it possible, here is an [example for root cause analysis](https://issues.apache.org/jira/browse/KYLIN-4153)
* Set the **Assignee** to yourself, if you plan to work on the task

An example of JIRA:
![](images/ISSUE_TEMPLATE.png)

### Step 3. Discuss your plan with others
Always remember, **[community over code!](http://theapacheway.com/community-over-code/)**

Be sure to discuss your idea, plan, design with the community before jumping into coding. Without involving the community, it is just a personal project for you.

Working with community has the following benefits:
- The knowledge is more important than the code. If no one has the knowledge to maintain the code, the code is dead. Working with community passes on the knowledge, and that keeps the open source alive.
- For big changes, aligning the architecture and high level designs is critical. Or you may face questions or even rejections when later submitting the code.
- You may find volunteers who want to or have been working on the same thing. With more helping hands, it usually results better code quality, better testing, and better documentation.

In ASF, **community discussions take place in [the mailing lists](https://www.apache.org/foundation/mailinglists.html)**.
- [How to subscribe to mailing list](how_to_subscribe_mailing_list), check the guide if have not yet.
  - Note the [the confirmation email](https://www.apache.org/foundation/mailinglists.html#request-confirmation) may show in trash mailbox for some users.
- For tips and examples on email discussion, here is a [guide about asking good question](https://infra.apache.org/contrib-email-tips.html#usefulq) and here is an [good example of development proposal](https://lists.apache.org/thread/gtcntp4s8k0fz1d4glospq15sycc599x).


### Step 4. Make code changes and PR

A few notes about making code changes:
* Though no strict code style at the moment, the general rule is keep consistent with existing files. E.g. use 4-space indent for java files.
* Add test case for your code change is MANDATORY.
* Make sure [all tests pass](how_to_test.md) before creating the pull request. This will ensure your change is in good quality and does not break others work.
* Since your code will be reviewed, read the [Code Review Guidelines](#CodeReviewGuideline) in advance. Follow the good design principles before you are asked to do so.

Once code changes are done, you can submit the changes in a new pull request:

* Push your code changes into a development branch

  ```shell
  # After making changes to the code ...
  git commit -m "KYLIN-XXXX COMMIT SUMMARY"
  git push <your_fork> <development_branch>
  ```

* In GitHub, click the ___Compare & pull request___ button to initiate a new pull request.

  ![](images/how-to-contribute-02.png)

* Kylin community requires a PR template. Fill out the info and click ___Create pull request___ to create a new pull request.

  :::info Kylin PR Template
  Pass on the knowledge, not just the code.
  1. In the `Proposed changes`, describe the __why__ and __how__ of your change.
  2. Choose the right `Types of changes` and check the `Checklist`.
  3. Double check the target branch is correct, or the code cannot merge.
  :::

  ![](images/how-to-contribute-03.png)

### Step 5. Do code review with a reviewer

A Kylin committer will get notified and review your pull request following the [Code Review Guidelines](#CodeReviewGuideline). You may be contacted through PR or JIRA comments. Or even meeting invitations if the discussion becomes involved.

Please wait patiently while the review takes place. Most committers have their own jobs and can only serve the community in the spare time. Try add a comment in the PR if you want to give it a small push.

When all is set, your pull request will be merged into the target branch by the committer.


### Step 6. Make related document changes

Document is as important as code, if you want others to know about a new feature or a critical bug has been resolved. Remember, **[community over code!](http://theapacheway.com/community-over-code/)**

Please follow the [How to write document](./how_to_write_doc) guide to write documents for each bigger code change. And be sure to mention the document change in your code PR, such that reviewer knows you are following the best practices.


-----

## Guide for Reviewer

### <span id="CodeReviewGuideline">Code Review Guideline</span>
The reviewer needs to review the patch from the following perspectives:

* _Functionality_: the patch MUST address the issue and has been verified by the contributor before submitting for review.
* _Test coverage_: the change MUST be covered by a UT or the Integration test, otherwise it is not maintainable. Execptional case includes GUI, shell script, etc.
* _Performance_: the change SHOULD NOT downgrade Kylin's performance.
* _Metadata compatibility_: the change should support old metadata definition. Otherwise, a metadata migration tool and documentation is required.
* _API compatibility_: the change SHOULD NOT break public API's functionality and behavior; If an old API need be replaced by the new one, print warning message there.
* _Documentation_: if the Kylin document need be updated together, create another JIRA with "Document" as the component to track. In the document JIRA, attach the doc change patch which is againt the "document" branch.

:::info Rules must be obeyed
A patch which doesn't comply with the above rules may not get merged.
:::

### Patch +1 Policy

Patches that fit within the scope of a single component require, at least, a +1 by one of the component’s owners before commit. If owners are absent — busy or otherwise — two +1s by non-owners but committers will suffice.

Patches that span components need at least two +1s before they can be committed, preferably +1s by owners of components touched by the x-component patch.

Any -1 on a patch by anyone vetoes a patch; it cannot be committed until the justification for the -1 is addressed.