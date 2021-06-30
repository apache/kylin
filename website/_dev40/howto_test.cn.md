---
layout: dev40-cn
title:  "如何测试"
categories: development
permalink: /cn/development40/howto_test.html
---

一般来说，应该有单元测试来涵盖个别 classes；必须有集成测试来涵盖端到端的场景，如构建，合并和查询。单元测试必须独立运行（不需要外部沙箱）。

## 测试 v4.x

* `mvn clean test` 运行单元测试，它的测试覆盖范围有限。
    * 单元测试没有额外的依赖且能在任何机器上运行。
    * 单元测试不覆盖端到端的场景，如构建，合并和查询。
    * 单元测试只需几分钟即可完成。
* `mvn clean test -DskipRunIt=false` 运行集成测试，有很好的覆盖率。。
    * 集成测试从生成随机数据开始，然后构建 Cube、合并 Cube，最后查询结果并与 Spark 引擎进行比较。
    * 集成测试需要一个小时左右才能完成。

如果您的代码改动很小那么只需要运行 UT，使用： 
`mvn test`
如果您的代码改动涉及代码较多，那么需要运行 UT 和 IT，使用：
`mvn clean test -DskipRunIt=false`

