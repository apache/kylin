---
layout: docs-cn
title:  "Superset"
categories: tutorial
permalink: /cn/docs/tutorial/superset.html
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

### Kyligence Insight for Superset
定制版的 Superset：Kyligence Insight for Superset，使得 Kylin 的用户多了一种选择。具体的安装步骤请在 github 上查看 [这个项目](https://github.com/Kyligence/Insight-for-Superset)。

##### 相比原生 Superset, 提供了如下增强功能：
1. 统一用户管理，用户无需在 "Superset" 上额外创建用户和赋予权限，统一在 Kylin 后端管理用户访问权限，直接使用 Kylin 账户登录 Superset。
2. 一键同步 Kylin Cube，无需在 Superset 端重新定义数据模型，直接查询 Cube.
3. 支持多表连接模型，支持 inner join 和 outer join.
4. Docker 容器化部署 Superset，一键启动，降低部署和升级门槛。
5. 自动适配 Kylin 查询语法。