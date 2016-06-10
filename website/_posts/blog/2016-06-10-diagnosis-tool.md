---
layout: post-blog
title:  Diagnosis Tool Introduction
date:   2016-06-10 23:20:00
author: Dong Li
categories: blog
---

## Introduction

Since Apache Kylin 1.5.2, there's a diagnosis tool on Web UI, which aims to help Kylin admins to extract diagnostic information for fault analysis and performance tunning.

### Project Diagnosis
When user met issues about query failure, slow queries, metadata management and so on, he could click the 'Diagnosis' button on System tabpage.

![](/images/blog/diag1.png)

Several seconds later, a diagnosis package will be avaliable to download from web browser, which contains kylin logs, metadata, configuration etc. Users could extract the package and analyze. Also when users asking help from experts in his team, attaching the package would raise the communication effeiciency.

### Job Diagnosis
When user met issues about jobs, such as cubing failed, slow job and so on, he could click the 'Diagnosis' button in the Job's Action menu.

![](/images/blog/diag2.png)

The same with Project Diagnosis, a diagnosis package will be downloaded from web browser, which contains related logs, MR job info, metadata etc. User could use it for analysis or ask for help. 