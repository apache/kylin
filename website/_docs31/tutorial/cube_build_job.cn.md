---
layout: docs31-cn
title: "Cube 构建和 Job 监控"
categories: 教程
permalink: /cn/docs31/tutorial/cube_build_job.html
version: v1.2
since: v0.7.1
---

### Cube建立

首先，确认你拥有你想要建立的 cube 的权限。

1. 在 `Models` 页面中，点击 cube 栏右侧的 `Action` 下拉按钮并选择 `Build` 操作。

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/1 action-build.png)

2. 选择后会出现一个弹出窗口，点击 `Start Date` 或者 `End Date` 输入框选择这个增量 cube 构建的起始日期。

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/3 date.PNG)

3. 点击 `Submit` 提交请求。成功之后，你将会在 `Monitor` 页面看到新建的 job。

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/4 jobs-page.png)

4. 新建的 job 是 “pending” 状态；一会儿，它就会开始运行并且你可以通过刷新 web 页面或者点击刷新按钮来查看进度。

    ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/5 job-progress.png)

5. 等待 job 完成。期间如要放弃这个 job ，点击 `Actions` -> `Discard` 按钮。

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/6 discard.png)

6. 等到 job 100%完成，cube 的状态就会变为 “Ready”, 意味着它已经准备好进行 SQL 查询。在 `Model` 页，找到 cube，然后点击 cube 名展开消息，在 “Storage” 标签下，列出 cube segments。每一个 segment 都有 start/end 时间；Hbase 表的信息也会列出。

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/10 cube-segment.png)

如果你有更多的源数据，重复以上的步骤将它们构建进 cube。

### Job监控

在 `Monitor` 页面，点击job详情按钮查看显示于右侧的详细信息。

![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/7 job-steps.png)

job 详细信息为跟踪一个 job 提供了它的每一步记录。你可以将光标停放在一个步骤状态图标上查看基本状态和信息。

![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/8 hover-step.png)

点击每个步骤显示的图标按钮查看详情：`Parameters`、`Log`、`MRJob`。

* Parameters

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 parameters.png)

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 parameters-d.png)

* Log

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 log.png)

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 log-d.png)

* MRJob(MapReduce Job)

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 mrjob.png)

   ![](/images/tutorial/1.5/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 mrjob-d.png)

