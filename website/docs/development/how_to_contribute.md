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

Apache Kylin is always looking for contributions of not only code, but also usage document, performance report, Q&A etc. All kinds of contributions pave the way towards a Kylin Committer. There is opportunity for everyone, especially for those come from analysis and solution background, due to the lacking of content from user and solution perspective.

### Source Branches
Both code and document are under Git source control. Note the purpose of different branches.

* `kylin5`: Development branch for v5.x
* `doc5.0`: Document branch for v5.x
* `main`: Maintenance branch for v4.x
* `kylin3`: Maintenance branch for v2.x
* `document`: Document branch for v4.x and before

-----

## Guide for Contributor 

:::info 
Want to know what do different role(like contributor, committer and PMC member) means in ASF? Check this for [official explanation](https://www.apache.org/foundation/how-it-works.html#roles) .
:::

### Overall steps
1. Fork [Apache Kylin Repo](https://github.com/apache/kylin) to your repository.
2. Clone the fork repo to your local. It is recommended to [create a pull request from a fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).
3. Create a new development branch locally.
4. [Setup development environment](how_to_debug_kylin_in_ide.md)
5. [Pick or Create a JIRA](#open_issue), describe the feature/enhancement/bug.
6. [Discuss with others in mailing list](#mailing_list) or issue comments, make sure the proposed changes fit in with what others are doing and have planned for the project.
7. Make code changes in your development branch
   - [ ] No strict code style at the moment, but the general rule is keep consistent with existing files. E.g. use 4-space indent for java files.
   - [ ] Add test case for your code change as much as possible.
   - [ ] Make sure [Run tests](how_to_test.md) can get success, this will ensure your change is in good quality and does not break anything.
   - [ ] Sufficient unit test and integration test is a mandatory part of code change.
8. Read [CODE REVIEW guidelines](#CodeReviewGuideline) and check if your code does not adhere to the guidelines, you may be asked to redo some work later if you forgot them.
9. [Create a pull request](#open_pull_request) for you code change.
10. If you need to update doc, please check out [How to Write Document](./how_to_write_doc) for help.


#### <span id="open_issue">Step 4: Pick or create a task</span>
There are open tasks waiting to be done, tracked by [KYLIN JIRA](http://issues.apache.org/jira/browse/KYLIN).
If you want to create a new JIRA for bug or feature, remember to provide enough information for the community:

* A well **summary** for the problem or feature
* A detail **description**, which may include:
    - the environment of this problem occurred
      - Kylin version
      - Hadoop/Spark version ...
    - the steps to reproduce the problem
    - the error trace or log files (as attachment)
    - the metadata of the model or cube (as attachment)
* **Affected version**: which Kylin you're using.

#### <span id="mailing_list">Step 5: Discuss your proposal in mailing list</span>
Do not forget to discuss in [mailing list](https://www.apache.org/foundation/mailinglists.html) before working on a big task.
For how to discuss your idea/proposal in mailing list, please check this example : [Example for developer's proposal](https://lists.apache.org/thread/gtcntp4s8k0fz1d4glospq15sycc599x) .

:::caution subscribe a mailing list
1. Before you sending mail to mailing list, please make sure you have subscribed a mailing list. Please [check this guide](https://www.apache.org/foundation/mailinglists.html#subscribing) if you don't know how to subscribe a mailing list.
2. If you do not [receive the confirmation email](https://www.apache.org/foundation/mailinglists.html#request-confirmation) after sending email to the mail list, the email maybe is shown in your trash mail.
:::

These are the mailing lists that have been established for kylin project. For each list, there is a subscribe, unsubscribe, and an archive link.

|    Mailing List   |   Subscribe Link  | Unsubscribe Link  |   Archive Link    |
|:-----------------:|:-----------------:|:-----------------:|:-----------------:|
| user              | [subscribe](mailto:user-subscribe@kylin.apache.org)  | [unsubscribe](mailto:user-unsubscribe@kylin.apache.org)  | [mail archive](https://lists.apache.org/list.html?user@kylin.apache.org) |
| dev               | [subscribe](mailto:dev-subscribe@kylin.apache.org)   | [unsubscribe](mailto:dev-unsubscribe@kylin.apache.org)   | [mail archive](https://lists.apache.org/list.html?dev@kylin.apache.org) |
| issue             | [subscribe](mailto:issue-subscribe@kylin.apache.org) | [unsubscribe](mailto:issue-unsubscribe@kylin.apache.org) | [mail archive](https://lists.apache.org/list.html?issue@kylin.apache.org) |

#### <span id="open_pull_request">Step 9: Create a pull request</span>

* Push your code change to your personal repo

Now you can make changes to the code. Then you need to push it to **development_branch**:

```shell
# After making changes to the code ...
git commit -m "KYLIN-0000 COMMIT SUMMARY"
git push origin development_branch
```

* Click the ___Compare & pull request___ button.

Once you push the changes to your repo, the Compare & pull request button will appear in GitHub.
![](images/how-to-contribute-02.png)

* Click ___Create pull request___ to open a new pull request.


:::caution Before you create pull request
1. Please add a detailed description in the `Proposed changes` of a pull request.
2. Click the `Types of changes` that you have made.
3. Check the `Checklist`.
:::

![](images/how-to-contribute-03.png)


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

:::caution
A patch which doesn't comply with the above rules may not get merged.
:::

### Patch +1 Policy

Patches that fit within the scope of a single component require, at least, a +1 by one of the component’s owners before commit. If owners are absent — busy or otherwise — two +1s by non-owners but committers will suffice.

Patches that span components need at least two +1s before they can be committed, preferably +1s by owners of components touched by the x-component patch.

Any -1 on a patch by anyone vetoes a patch; it cannot be committed until the justification for the -1 is addressed.