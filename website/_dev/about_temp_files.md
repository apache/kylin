---
layout: dev
title:  "About Temp Files"
categories: development
permalink: /development/about_temp_files.html
---

As we reviewed the code we found that Kylin left lots of garbage files in:

* Local file system of the CLI
* HDFS
* Local file system of the hadoop nodes.

A ticket was opened to track this issue:
[https://issues.apache.org/jira/browse/KYLIN-926](https://issues.apache.org/jira/browse/KYLIN-926)

For future developments, please:

* Whenever you want to create temp files at Local, choose
File.createTempFile or use the folder:
BatchConstants.CFG_KYLIN_LOCAL_TEMP_DIR(/tmp/kylin), do not randomly use
another folder in /tmp, it will end up a mess, and look unprofessional.
* Whenever you create temp files at Local, remember to delete it after
using it. It's best to use FileUtils.forceDelete, as it also works for
deleting folders. Try avoid deleteOnExit, in case Kylin exits abnormally.
* Whenever you want to create files in HDFS, try to create it under
kylin.hdfs.working.dir or BatchConstants.CFG_KYLIN_HDFS_TEMP_DIR, and
remember to delete it after it is no longer useful. Try avoid throwing
everything into hdfs:///tmp and leave it as garbage.
