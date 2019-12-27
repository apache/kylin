---
layout: docs31-cn
title:  "Superset"
categories: tutorial
permalink: /cn/docs31/tutorial/superset.html
---
### Apache Kylin 与 Apache Superset 的集成

##### 简介
Apache Superset（incubating）是一个现代化的企业级商业智能 Web 应用程序。Superset 的整个后端是基于 Python 开发的，用到了 Flask，Pandas，SqlAlchemy。可以与 Kylin Python Client 集成。

##### Apache Superset 功能
* 丰富的数据可视化集
* 易于使用的界面，用于探索和可视化数据
* 创建和共享仪表板
* 与主要身份验证提供程序（数据库，OpenID，LDAP，通过 Flask AppBuilder 的 OAuth 和 REMOTE_USER）集成的企业级身份验证
* 一种可扩展的，高粒度的安全/权限模型，对于可以访问个人特征和数据集的用户允许使用复杂的规则
* 一个简单的语义层，允许用户通过定义哪些字段应显示在哪个下拉列表以及哪些聚合和功能度量标准可供用户使用来控制数据源在 UI 中的显示方式
* 通过 SQLAlchemy 与大多数 SQL 的 RDBMS 集成

##### 集成的好处
Apache Kylin 和 Apache Superset 都是以为其用户提供更快和可交互式的分析的目的而构建。由于预先计算的 Kylin Cube，在 PB 级数据集上这两个开源项目的结合可以将这个目标变为现实。

##### 集成的步骤
1. 安装 Apache Kylin
2. 成功的 build cube
3. 安装 Apache Superset 并进行初始化
4. 在 Apache Superset 中连接 Apache Kylin
5. 配置一个新的数据源
6. 测试及查询

详细的集成步骤，请查看 [这篇文章](http://kylin.apache.org/blog/2018/01/01/kylin-and-superset/)。

##### 其它功能
Apache Superset 也支持导出 CSV, 共享, 以及查看 SQL 查询。
