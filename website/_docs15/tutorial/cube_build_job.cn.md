---
layout: docs15-cn
title:  Kylin Cube 建立和Job监控教程
categories: 教程
permalink: /cn/docs15/tutorial/cube_build_job.html
version: v1.2
since: v0.7.1
---

### Cube建立
首先，确认你拥有你想要建立的cube的权限。

1. 在`Cubes`页面中，点击cube栏右侧的`Action`下拉按钮并选择`Build`操作。

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/1 action-build.png)

2. 选择后会出现一个弹出窗口。

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/2 pop-up.png)

3. 点击`END DATE`输入框选择增量构建这个cube的结束日期。

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/3 end-date.png)

4. 点击`Submit`提交请求。

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/4 submit.png)

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/4.1 success.png)

   提交请求成功后，你将会看到`Jobs`页面新建了job。

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/5 jobs-page.png)

5. 如要放弃这个job，点击`Discard`按钮。

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/6 discard.png)

### Job监控
在`Jobs`页面，点击job详情按钮查看显示于右侧的详细信息。

![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/7 job-steps.png)

job详细信息为跟踪一个job提供了它的每一步记录。你可以将光标停放在一个步骤状态图标上查看基本状态和信息。

![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/8 hover-step.png)

点击每个步骤显示的图标按钮查看详情：`Parameters`、`Log`、`MRJob`、`EagleMonitoring`。

* Parameters

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 parameters.png)

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 parameters-d.png)

* Log
        
   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 log.png)

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 log-d.png)

* MRJob(MapReduce Job)

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 mrjob.png)

   ![]( /images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 mrjob-d.png)
