---
layout: post-blog
title:  你离可视化酷炫大屏只差一套 Kylin + Davinci
date:   2019-11-29 15:00:00
author: 高亚山、夏邢
categories: cn_blog
---

Kylin 提供与 BI 工具的整合能力，如 Tableau，PowerBI/Excel，MSTR，QlikSense，Hue 和 SuperSet。但就可视化工具而言，Davinci 良好的交互性和个性化的可视化大屏展现效果，使其与 Kylin 的结合能让大部分用户有更好的可视化分析体验。

Davinci 是国内开源的大数据可视化平台，是一款基于 web，提供一站式数据可视化解决方案的平台，Java 系。用户只需在可视化 UI 上简单配置即可服务多种数据可视化应用，并支持高级交互/行业分析/模式探索/社交智能等可视化功能。详情请访问其官方网站（https://edp963.github.io/davinci/）。

### 下载与安装
宜信在 2018 年 4 月发布了 Davinci 的第一个正式版本 V0.1.0，目前为止 Davinci 的正式发布版本是 v0.2.1，其次就是 v0.3 系列的测试版。Davinci 自 0.2.1 版本之后开始支持对 Kylin 的连接。通过对比可以发现，0.2 版本只是简单地实现了数据可视化报表，其功能不全，用户交互性差。但随后的 0.3 版本在不断地完善平台功能，可以说使用过程中体验感良好，功能比较齐全。并且官方在不断地进行版本的更新中，所以对于初次接触 Davinci 和想拥有自定义仪表盘和大屏效果的人群，更建议使用最新版 v0.3 系列。

部署之前，安装环境要包含 JDK，MySQL，Mail Server，PhantomJs。然后，到官网给定的 github 网站上下载最新发布的软件包，解压到自定义的安装目录下，并配置 davinci 的环境变量。同时，修改 bin 目录下 initdb.sh 中数据库信息为要初始化的数据库，运行脚本初始化数据库：sh bin/initdb.sh

之后，进入到config文件夹下，将 application.yml.example 重命名为 application.yml 后开始配置。如：访问地址和端口号（默认端口号为 8080，可自定义），数据源等配置。详细的配置部署请参考官网说明（https://edp963.github.io/davinci/deployment.html），完成部署后。在  bin 目录下执行 sh start-server.sh 命令启动 Davinci 服务。

最后，打开浏览器，访问地址：http://{配置的地址}:{配置的端口号}，即可进入 Davinci，新用户进行注册即可使用该服务。
![](/images/blog/davinci/login.png)
<center>登陆界面</center>

### 连接 Kylin
Davinci 的官方网站介绍其支持 JDBC 数据源连接，这就为 kylin 的连接提供了可能。Davinci 默认可支持的数据源不包括 kylin，但是提供了自定义数据源配置文件。首先，进入 lib 目录下添加 kylin-jdbc 包，其次，进入config目录下，更改datasource_driver.yml.example文件名为datasource_driver.yml 使其生效，并在文件里配置Kylin 相关信息，如下：
```
kylin:
   name: kylin
   desc: kylin
   driver: org.apache.kylin.jdbc.Driver
   keyword_prefix: \"
   keyword_suffix: \"
   alias_prefix: \"
   alias_suffix: \"
```
重启服务，使配置生效。

最后，可做一个简单的数据连接测试来验证是否连接成功。在 Source 部分添加数据源 kylin 并填写相关的用户名，密码，url 地址等信息来进行连接测试，如下图所示：
![](/images/blog/davinci/connect.png)
<center>数据源连接</center>
连接成功后，接着在 View 层输入查询 SQL 语句，点击右下角的执行按钮即可。如下图：
![](/images/blog/davinci/query.png)

### 制作数据仪表盘及大屏展示
Davinci 为用户提供了两种自定义的报表形式，一种是常见的可以自由布局的报表（dashbord），除此之外，还提供了用户可自定制的大屏展现形式（display）。

我们可以利用 Widget 层丰富的图表来展现 View 层的数据，进而根据需求制作不同展现形式的报表。那么在 Widget 层，我们可以通过拖拽的方式，为不同维度的数据选择适合的图像进行展示。仪表盘（Dashbord）的展现如下图：
![](/images/blog/davinci/dashboard.png)
<center>数据仪表盘</center>
如果用户需要更加酷炫的大屏展现形式，我们可以使用 Display 来手动定制报表的展现形式，如下图：
![](/images/blog/davinci/setting.png)
<center>Display 功能区</center>
其中：
网格区域：布置画布区域，效果展现区域
蓝色区域：添加 Widget 层制作的图表，添加过程中我们可以自定义定时刷新数据；
红色区域：添加辅助图形，如：文本编辑框，矩形；
绿色区域：画布上不同元素的图层设置；
黑色区域：大屏的背景设置区域，包括屏幕的尺寸，缩放规则，背景颜色，添加背景图片，截取封皮。

通过这些功能，我们可以轻轻松松地定制出符合场景需求的动态大屏展示效果。如下示例：
![](/images/blog/davinci/monitor.png)

### 总结
Kylin 本身也提供简单的图表展示，例如：饼图，柱状图等。但并不能满足大多数用户的需求，通过 Kylin+Davinci 的结合，我们可以将 Kylin 快速查询特点与 Davinci 多样化和个性化的展示效果充分的整合起来，从而满足更多用户的需求，做好大数据分析最后一站的服务工作。

那么本次选择 Davinci 来做数据可视化展现，一是由于其自身丰富的功能和一站式的可视化分析展现。再者，其开源的性质和开发的语言，为大多数开发者提供了更多的可能，如果你喜欢，那么你就可以在其基础上进行二次开发，来满足自己的场景。