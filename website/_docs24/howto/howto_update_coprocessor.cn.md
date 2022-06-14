---
layout: docs24-cn
title:  "更新 Coprocessor"
categories: howto
permalink: /cn/docs24/howto/howto_update_coprocessor.html
---

Kylin 利用 HBase coprocessor 来优化查询性能。新版本发布后，RPC 协议可能会发生变化，因此用户需要将协处理器重新部署到 HTable。

以下 CLI 工具可以更新 H Base Coprocessor：

{% highlight Groff markup %}
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI default all
{% endhighlight %}
