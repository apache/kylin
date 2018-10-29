---
layout: docs-cn
title:  备份元数据
categories: 帮助
permalink: /cn/docs/howto/howto_backup_metadata.html
---

Kylin将它全部的元数据（包括cube描述和实例、项目、倒排索引描述和实例、任务、表和字典）组织成层级文件系统的形式。然而，Kylin 使用 HBase 来存储元数据，而不是一个普通的文件系统。如果你查看过Kylin的配置文件（kylin.properties），你会发现这样一行：

{% highlight Groff markup %}
## The metadata store in hbase
kylin.metadata.url=kylin_metadata@hbase
{% endhighlight %}

这表明元数据会被保存在一个叫作 “kylin_metadata”的htable 里。你可以在 hbase shell 里 scan 该 htbale 来获取它。

## 使用二进制包来备份 metadata

有时你需要将 Kylin 的 metadata store 从 hbase 备份到磁盘文件系统。在这种情况下，假设你在部署 Kylin 的 hadoop 命令行（或沙盒）里，你可以到KYLIN_HOME并运行：

{% highlight Groff markup %}
./bin/metastore.sh backup
{% endhighlight %}

来将你的元数据导出到本地目录，这个目录在KYLIN_HOME/metadata_backps下，它的命名规则使用了当前时间作为参数：KYLIN_HOME/meta_backups/meta_year_month_day_hour_minute_second 。

## 使用二进制包来恢复 metadata

万一你发现你的元数据被搞得一团糟，想要恢复先前的备份：

首先，重置 metatdara store（这个会清理 Kylin 在 HBase 的 metadata store的所有信息，请确保先备份）：

{% highlight Groff markup %}
./bin/metastore.sh reset
{% endhighlight %}

然后上传备份的元数据到 Kylin 的 metadata store：
{% highlight Groff markup %}
./bin/metastore.sh restore $KYLIN_HOME/meta_backups/meta_xxxx_xx_xx_xx_xx_xx
{% endhighlight %}

## 有选择地恢复 metadata (推荐)
如果只更改了几个元数据文件，管理员只需选择要还原的这些文件，而不必覆盖所有元数据。 与完全恢复相比，这种方法更有效，更安全，因此建议使用。

创建一个新的空目录，然后根据要还原的元数据文件的位置在其中创建子目录; 例如，要恢复多维数据集实例，您应该创建一个“cube”子目录：

{% highlight Groff markup %}
mkdir /path/to/restore_new
mkdir /path/to/restore_new/cube
{% endhighlight %}

将要还原的元数据文件复制到此新目录：

{% highlight Groff markup %}
cp meta_backups/meta_2016_06_10_20_24_50/cube/kylin_sales_cube.json /path/to/restore_new/cube/
{% endhighlight %}

此时，您可以手动修改/修复元数据。

从此目录还原：

{% highlight Groff markup %}
cd $KYLIN_HOME
./bin/metastore.sh restore /path/to/restore_new
{% endhighlight %}

只有在此文件夹中的文件才会上传到Kylin Metastore。 同样，在恢复完成后，单击 Web UI 上的“Reload Metadata”按钮以刷新缓存。

## 在开发环境备份/恢复元数据

在开发调试 Kylin 时，典型的环境是一台装有 IDE 的开发机上和一个后台的沙盒，通常你会写代码并在开发机上运行测试案例，但每次都需要将二进制包放到沙盒里以检查元数据是很麻烦的。这时有一个名为 SandboxMetastoreCLI 工具类可以帮助你在开发机本地下载/上传元数据。

## 从 metadata store 清理无用的资源
随着运行时间增长，类似字典、表快照的资源变得没有用（cube segment被丢弃或者合并了），但是它们依旧占用空间，你可以运行命令来找到并清除它们：

首先，运行一个检查，这是安全的因为它不会改变任何东西：
{% highlight Groff markup %}
./bin/metastore.sh clean
{% endhighlight %}

将要被删除的资源会被列出来：

接下来，增加 “--delete true” 参数来清理这些资源；在这之前，你应该确保已经备份 metadata store：
{% highlight Groff markup %}
./bin/metastore.sh clean --delete true
{% endhighlight %}
