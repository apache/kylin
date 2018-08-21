---
layout: dev-cn
title:  关于临时文件
categories: development
permalink: /cn/development/about_temp_files.html
---

在我们查看代码时，我们发现 Kylin 在下列位置留下了大量垃圾文件：

* 客户端的本地文件系统
* HDFS
* hadoop 节点的本地文件系统。

开了一个单子来追踪这个 issue:
[https://issues.apache.org/jira/browse/KYLIN-926](https://issues.apache.org/jira/browse/KYLIN-926)

为了将来的开发，请：

* 每当您想在本地创建临时文件时，请选择
  File.createTempFile 或使用文件夹：
  BatchConstants.CFG_KYLIN_LOCAL_TEMP_DIR(/tmp/kylin)，不要随意使用
  /tmp 中的另一个文件夹，它会变得一团糟，看起来不专业。
* 每当您在本地创建临时文件时，请记得在使用之后删除它
  使用它。最好使用 FileUtils.forceDelete，因为它也适用于
  删除文件夹。如果 Kylin 异常退出，请尽量避免使用 deleteOnExit 方法。
* 每当您想要在 HDFS 中创建文件，请尝试在
  kylin.hdfs.working.dir 或 BatchConstants.CFG_KYLIN_HDFS_TEMP_DIR 下创建文件，以及
  记得在它不再有用之后删除它。尽量避免投掷
  一切到 hdfs:///tmp 并将其留作垃圾。
