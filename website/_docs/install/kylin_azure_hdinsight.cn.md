---
layout: docs-cn
title:  "在Azure HDInsight上一键部署Kylin"
categories: install
permalink: /cn/docs/install/kylin_azure_hdinsight.html
---

本文介绍了如何在HDInsight上一键部署Kylin

部署脚本源码位于: [Github Repository](https://github.com/Kyligence/Iaas-Applications/tree/master/apache-kylin)

## 1. 准备一个HDInsight集群
首先，要有一个可用的HDInsight集群（现有集群或新建集群均可），集群的类型必须是HBase，HDInsight的版本必须是大于等于3.2且小于4.0。
![](/images/Kylin-On-Azure/createHDInsight.png)

## 2. 部署Kylin
点击 <a href="https://portal.azure.cn/#create/Microsoft.Template/uri/https%3a%2f%2fraw.githubusercontent.com%2fKyligence%2fIaas-Applications%2fmaster%2fapache-kylin%2fazuredeploy.json" target="_blank">
		<img src="http://azuredeploy.net/deploybutton.png"/>
	</a>，填写如下图所示参数后，即可开始部署Kylin。其他参数可根据需要自行修改。
![](/images/Kylin-On-Azure/fillclustername.png)

## 3. 登录至Kylin
部署完毕后，在HDInsight管理界面下的application页面里，可以看到已部署应用的访问链接。复制该链接，并在最后追加 **/kylin后**，即可访问Kylin。
![](/images/Kylin-On-Azure/portalinapplication.png)

## 4. 开始使用
Kylin在部署时，默认导入了Sample Cube数据。如果你是第一次使用Kylin，可以用Sample Cube进行快速体验和学习。如果你对Kylin比较了解，那么可以直接导入自己的数据进行分析。使用详情可以参阅：<a href="http://kylin.apache.org/docs/tutorial/web.html" target="_blank">Kylin 官方文档</a>

# 如何自定义properties文件
部署时默认使用了templates目录下的[kylin.properties](https://github.com/Kyligence/Iaas-Applications/blob/master/apache-kylin/templates/kylin.properties)。如果你想改变该配置，方法如下：
1. 登录至Kylin部署节点。部署节点的登录信息可以在HDInsight管理界面下的**SSH+Cluster login**中找到；
![](/images/Kylin-On-Azure/sshinfo.png)

2. **sudo su root** 切换至root用户；

3. 通过**vim ~/apache-kylin-{version}-bin-hbase1x/conf/kylin.properties**进行修改；

4. 修改完毕后，重启Kylin，使新配置生效。