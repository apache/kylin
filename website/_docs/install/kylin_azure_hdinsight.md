---
layout: docs
title:  "Install Kylin on Azure HDInsight"
categories: install
permalink: /docs/install/kylin_azure_hdinsight.html
---

This document introduces how to deploy Kylin on HDInsight by one click.

You can get more detail from this [Github Repository](https://github.com/Kyligence/Iaas-Applications/tree/master/apache-kylin)

## 1. Prepare an HDInsight cluster
First, you need to have an HDInsight cluster which the type is "HBase". The HDInsight version must greater than 3.2(inlucde) and less than 4.0(exclude). Either existing cluster or new-created clustser is OK.
![](/images/Kylin-On-Azure/createHDInsight.png)

## 2. Deploy Kylin
Click <a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3a%2f%2fraw.githubusercontent.com%2fKyligence%2fIaas-Applications%2fmaster%2fapache-kylin%2fazuredeploy.json" target="_blank">
		<img src="http://azuredeploy.net/deploybutton.png"/>
	  </a> to deploy Apache Kylin. You will have to fill one param, just like the picture shown below. Other params can also be modified as you requried.  
![](/images/Kylin-On-Azure/fillclustername.png)

## 3. Login to Kylin 
After finish deploying Kylin, you can find the Kylin host in application page in HDInsight. Copy the link of Portal to your browser and append ***/kylin*** in the end. Then press the enter to access Kylin. The defualt username is **ADMIN** and the password is **KYLIN**. 
![](/images/Kylin-On-Azure/portalinapplication.png)

## 4. Use Kylin
By default, the Sample Cube has been loaded into Kylin during the deployment. If it's the first time you use Kylin, you can get quick start with Sample Cube. Also, if you're fimilar with Kylin, you can load your own data into Kylin. For more details about how to use Kylin, please refer <a href="http://kylin.apache.org/docs/tutorial/web.html" target="_blank">Kylin Documents</a>

# How to Customized the kylin.properites
By default, Kylin use the [kylin.properties](https://github.com/Kyligence/Iaas-Applications/blob/master/apache-kylin/templates/kylin.properties). If you want to modifiy it, you can do as follows:
1. Login to the node that deploy the Kylin. The ssh login info can be founded in **SSH+Cluster login** page in HDInsight.
![](/images/Kylin-On-Azure/sshinfo.png)

2. **sudo su root** to switch to root.

3. Modify the kylin.properties by using **vim ~/apache-kylin-{version}-bin-hbase1x/conf/kylin.properties**.

4. Restart the Kylin.