---
layout: docs15
title:  How to Update HBase Coprocessor
categories: howto
permalink: /docs15/howto/howto_update_coprocessor.html
---

Kylin leverages HBase coprocessor to optimize query performance. After new versions released, the RPC protocol may get changed, so user need to redeploy coprocessor to HTable.

There's a CLI tool to update HBase Coprocessor:

{% highlight Groff markup %}
$KYLIN_HOME/kylin.sh org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI $KYLIN_HOME/lib/kylin-coprocessor-*.jar all
{% endhighlight %}