---
layout: dev-cn
title:  如何贡献
categories: development
permalink: /cn/development/howto_contribute.html
---

Apache Kylin 一直寻求的不只是代码的贡献，还寻求使用文档，性能报告，问答等方面的贡献。所有类型的贡献都为成为 Kylin Committer 铺平了道路。每个人都有机会，尤其是那些有分析和解决方案背景的，因为缺少来自于用户和解决方案视角的内容。


## 源分支
代码和文档都在 Git 源代码控制之下。注意不同分支的用途。

* `master`: 新功能的主开发分支
* `2.[n].x`: 一些主要版本的维护分支
* `document`: 文档分支

## 组件及拥有者
Apache Kylin 有几个子组件。为了更好地帮助社区的发展，我们为每个组件安排了一个或多个组件负责人。 

- 组件负责人是志愿者（组件领域的专家）。负责人需要成为 Apache Kylin 提交者或 PMC。 

- 负责人将尝试审查其组件范围内的补丁。

- 负责人可以根据他的愿望和社区需求进行轮换。

- 在提名或投票新提交者时，提名者需要说明候选人可以成为哪个组件的负责人。

- 如果您已经是 Apache Kylin 提交者或 PMC 成员并希望成为组件负责人的志愿者，请给 dev 列表写信，我们将为您注册。 

- 所有项目计划，决策仍由 Apache Kylin PMC 管理。

- 如果您认为组件列表需要更新（添加，删除，重命名等），请给 dev 列表写信，我们将对其进行审核。

组件负责人列在了这个 Apache Kylin [JIRA components page](https://issues.apache.org/jira/projects/KYLIN?selectedItem=com.atlassian.jira.jira-projects-plugin:components-page) 页面中的 Description 字段位置。负责人列在“Description”字段中而不是“Component Lead”字段中，因为后者仅允许我们列出一个人，然而其鼓励组件具有多个负责人。

## 选择一个任务
这里有新创建的任务等待被完成，由 JIRA 追踪。为了让其容易被搜索，这里有一些过滤条件。

* 由李扬管理的[任务列表](https://issues.apache.org/jira/issues/?filter=12339895) 。
* 由 Ted Yu 创建的[任务列表](https://issues.apache.org/jira/issues/?filter=12341496)，重要的小的 bugs 且其中一些很容易被修复。
* 您也可以在 Kylin JIRA 中搜索标签 “newbie”。

在做大任务之前别忘了在[邮箱列表](/community/index.html)中讨论。

如果为 bug 或功能创建了新的 JIRA，请记住为社区提供足够的信息：

* 问题或功能的良好摘要
* 详细说明，可能包括：
	- 这个问题发生的环境 
	- 重现问题的步骤
	- 错误跟踪或日志文件（作为附件）
	- model 或 cube 的元数据
* 相关组件：我们将根据此选择安排审核人员。
* 受影响的版本：您正在使用的 Kylin 版本。

## 进行代码更改
* [搭建开发环境](/cn/development/dev_env.html)
* 提出 JIRA，描述功能/提升/bug
* 在邮件列表或 issue 评论中与其他人讨论，确保提议的更改符合其他人正在做的事情以及为项目规划的内容
* 在您的 fork 中进行修改
	* 目前没有严格的代码样式，但一般规则与现有文件保持一致。例如，对 java 文件使用 4 空格缩进。
	* 尽可能为代码更改添加测试用例。
	* 确保“mvn clean package”和“mvn test”能够获得成功。
	* 充分的单元测试和集成测试是代码更改的必要部分。 
* [运行测试](/cn/development/howto_test.html) 以确保您的更改质量良好且不会破坏任何内容。如果您的补丁生成不正确或您的代码不符合代码指南，则可能会要求您重做某些工作。
* 生成补丁并将其附加到相关的 JIRA。

## 生成 Patch
* 使用 `submit-patch.py`（推荐）创建 patches，上传到 jira 并可选择在 Review Board 上创建/更新评论。 Patch 名称自动格式化为(JIRA).(分支名称).(补丁号).patch，遵循 Yetus 的命名规则。

```
$ ./dev-support/submit-patch.py -jid KYLIN-xxxxx -b master -srb
```

* 用 -h 标志可以了解此脚本的详细用法信息。最有用的选项是：
	* -b BRANCH, --branch BRANCH : 指定用于生成 diff 的基本分支。如果未指定，则使用跟踪分支。如果没有跟踪分支，则会抛出错误。
	* -jid JIRA_ID, --jira-id JIRA_ID : 如果使用，则从 jira 中的附件推断下一个补丁版本并上传新补丁。脚本将要求 jira 用户名/密码进行身份验证。如果未设置，则将补丁命名为 .patch。
* 默认情况下，它还会创建/更新 review board。要跳过该操作，请使用 -srb 选项。它使用 jira 中的“Issue Links”来确定审核请求是否已存在。如果没有审核请求，则创建一个新请求并使用 jira 摘要，patch 说明等填充所有必填字段。此外，还将此评论的链接添加到 jira。
* 安装需要的 python 依赖，从 master 分支执行 `pip install -r dev-support/python-requirements.txt`。

* 或者，您也可以手动生成 patch。请首先使用 `git rebase -i`，将较小的提交组合（squash）为一个较大的提交。然后使用 `git format-patch` 命令生成 patch，有关详细指南，请参阅[如何使用 Git 创建和应用补丁](https://ariejan.net/2009/10/26/how-to-create-and-apply-a-patch-with-git/)。

## 代码审查
审核人员需要从以下角度审核 patch：

* _功能性_：patch 必须解决问题，并在提交审核之前已经过贡献者的验证。
* _测试范围_：更改必须由 UT 或集成测试覆盖，否则无法维护。执行案例包括 GUI，shell 脚本等。
* _性能_：改变不应该降低 Kylin 的性能。
* _元数据兼容性_：更改应支持旧元数据定义。否则，需要元数据迁移工具和文档。
* _API 兼容性_：更改不应该破坏公共 API 的功能和行为；如果需要用新 API 替换旧 API，请在那里打印警告消息。
* _文档_：如果需要同时更新 Kylin 文档，请创建另一个 JIRA，并将“Document”作为要跟踪的组件。在 JIRA 文档中，附加“文档”分支的文档更改 patch。

不符合上述规则的补丁可能无法合并。

## Patch +1 政策

在提交之前，适合单个组件范围的修补程序至少需要一个组件负责人的 +1。如果负责人不在 — 在忙或其他 — 两个非负责人（即两个提交者）的 +1，就足够了。

跨组件的 patch 在提交之前至少需要两个 +1s，最好由 x-component patch 涉及的组件负责人的 +1。

任何人都可以在 patch 上 -1，任何 -1 都可以否决补丁；在解决 -1 的理由之前，它不能被提交。


## 应用 Patch
* Committer 将审核 JIRA 中的 Pull Requests 和 Patches 的正确性，性能，设计，编码风格，测试覆盖率
* 必要时进行讨论和修改；
* committer 将代码合并到目标分支中
	* 对于 git patch，请使用“git am -s -3 patch-file”命令进行应用；
	* 如果是来自 github Pull Request，则需要添加“This closing＃”作为提交消息的一部分。这将允许 ASF Git bot 关闭 PR。
	* 使用 `git rebase` 确保合并结果是提交的简化。


## 进行文档更改
查看[如何写文档](/cn/development/howto_docs.html).

