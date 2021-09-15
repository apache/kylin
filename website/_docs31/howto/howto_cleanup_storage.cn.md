---
layout: docs31-cn
title:  清理存储
categories: 帮助
permalink: /cn/docs31/howto/howto_cleanup_storage.html
---

Kylin 在构建 cube 期间会在 HDFS 上生成中间文件；除此之外，当清理/删除/合并 cube 时，一些 HBase 表可能被遗留在 HBase 却以后再也不会被查询；虽然 Kylin 已经开始做自动化的垃圾回收，但不一定能覆盖到所有的情况；你可以定期做离线的存储清理：

步骤：
1. 检查哪些资源可以清理，这一步不会删除任何东西：
{% highlight Groff markup %}
export KYLIN_HOME=/path/to/kylin_home
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete false
{% endhighlight %}
请将这里的 (version) 替换为你安装的 Kylin jar 版本。
2. 你可以抽查一两个资源来检查它们是否已经没有被引用了；然后加上“--delete true”选项进行清理。
{% highlight Groff markup %}
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true
{% endhighlight %}
完成后，Hive 里的中间表, HDFS 上的中间文件及 HBase 中的 HTables 都会被移除。
3. 如果您想要删除所有资源；可添加 "--force true" 选项：
{% highlight Groff markup %}
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --force true --delete true
{% endhighlight %}
完成后，Hive 中所有的中间表, HDFS 上所有的中间文件及 HBase 中的 HTables 都会被移除。
