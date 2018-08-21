---
layout: dev-cn
title:  "Kylin Web 摘要"
categories: development
permalink: /cn/development/web_tech.html
---

### 项目依赖
* npm: 用于开发阶段安装 grunt 和 bower
* grunt: 构建并安装 kylin web
* bower: 管理 kylin 技术依赖

### 技术依赖
* Angular JS: kylin web 的基础支持
* ACE: sql 和 json 编辑器
* D3 JS: 绘制报表图表和 cube 图表
* Bootstrap: css 库

### 支持的用例:

###### Kylin web 支持 BI 工作流中各种角色的需求 

* 分析师：运行查询和检出结果
* Modeler：cube 设计，cube/job 操作和监视器
* 管理员：系统操作。

### 技术概览 
Kylin web 是一个基于 restful 服务构建的单页应用程序。Kylin web 使用 nodejs 中的工具来管理项目，并使用 AngularJS 来启用单页 Web 应用程序。Kylin web 使用来自 js 开源社区的流行技术，使其易于追赶和贡献。 

### 强调:
* 查询实用功能:
    * 表和列名称的 SQL 自动建议
    * 远程/本地查询保存
    * 数据网格通过简单的 BI 操作能支持百万级数据
    * 数据导出
    * 简单的数据可视化(折线图，柱状图，饼图)
* Cube 管理:
    * 精心设计的 cube 创建流程
    * cube 关系结构的可视化
    * 精心设计的 cube 访问管理
* Job 管理:
    * Job 步骤和日志监视器
    * 杀死
    * 恢复
* 有用的管理工具。
* 精致的外观和感觉。
