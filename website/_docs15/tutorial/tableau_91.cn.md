---
layout: docs15-cn
title:  Tableau 9 教程
categories: tutorial
permalink: /cn/docs15/tutorial/tableau_91.html
version: v1.2
since: v1.2
---

Tableau 9已经发布一段时间了，社区有很多用户希望Apache Kylin能进一步支持该版本。现在可以通过更新Kylin ODBC驱动以使用Tableau 9来与Kylin服务进行交互。

> Apache Kylin目前版本不支持原始数据的查询，部分查询会因此失败，导致应用程序发生异常，建议打上KYLIN-1075补丁包以优化查询结果的显示。

### Tableau 8.x 用户
请参考[Tableau 教程](./tableau.html)以获得更详细帮助。

### Install ODBC Driver
参考页面[Kylin ODBC 驱动程序教程](./odbc.html)，请确保下载并安装Kylin ODBC Driver __v1.2__. 如果你安装有早前版本，请卸载后再安装。 

### Connect to Kylin Server
在Tableau 9.1创建新的数据连接，单击左侧面板中的`Other Database(ODBC)`，并在弹出窗口中选择`KylinODBCDriver` 
![](/images/tutorial/odbc/tableau_91/1.png)

输入你的服务器地址、端口、项目、用户名和密码，点击`Connect`可获取有权限访问的所有项目列表。有关权限的详细信息请参考[Kylin Cube 权限授予教程](./acl.html).
![](/images/tutorial/odbc/tableau_91/2.png)

### 映射数据模型
在左侧的列表中，选择数据库`defaultCatalog`并单击”搜索“按钮，将列出所有可查询的表。用鼠标把表拖拽到右侧区域，就可以添加表作为数据源，并创建好表与表的连接关系
![](/images/tutorial/odbc/tableau_91/3.png)

### Connect Live
Tableau 9.1中有两种数据源连接类型，选择｀在线｀选项以确保使用'Connect Live'模式
![](/images/tutorial/odbc/tableau_91/4.png)

### 自定义SQL
如果需要使用自定义SQL，可以单击左侧｀New Custom SQL｀并在弹窗中输入SQL语句，就可添加为数据源.
![](/images/tutorial/odbc/tableau_91/5.png)

### 可视化
现在你可以进一步使用Tableau进行可视化分析：
![](/images/tutorial/odbc/tableau_91/6.png)

### 发布到Tableau服务器
如果希望发布到Tableau服务器, 点击`Server`菜单并选择`Publish Workbook`
![](/images/tutorial/odbc/tableau_91/7.png)

### 更多
请参考[Tableau 教程](./tableau.html)以获得更多信息


