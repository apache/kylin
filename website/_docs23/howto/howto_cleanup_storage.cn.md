---
layout: docs23-cn
title:  清理存储
categories: 帮助
permalink: /cn/docs23/howto/howto_cleanup_storage.html
---

Kylin在构建cube期间会在HDFS上生成中间文件；除此之外，当清理/删除/合并cube时，一些HBase表可能被遗留在HBase却以后再也不会被查询；虽然Kylin已经开始做自动化的垃圾回收，但不一定能覆盖到所有的情况；你可以定期做离线的存储清理：

步骤：
1. 检查哪些资源可以清理，这一步不会删除任何东西：
{% highlight Groff markup %}
export KYLIN_HOME=/path/to/kylin_home
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete false
{% endhighlight %}
请将这里的 (version) 替换为你安装的Kylin jar版本。
2. 你可以抽查一两个资源来检查它们是否已经没有被引用了；然后加上“--delete true”选项进行清理。
{% highlight Groff markup %}
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true
{% endhighlight %}
完成后，中间HDFS上的中间文件和HTable会被移除。
