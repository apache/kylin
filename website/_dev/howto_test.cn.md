---
layout: dev-cn
title:  "如何测试"
categories: development
permalink: /cn/development/howto_test.html
---

一般来说，应该有单元测试来涵盖个别 classes；必须有集成测试来涵盖端到端的场景，如构建，合并和查询。单元测试必须独立运行（不需要外部沙箱）。

## 测试 v1.5 及以上

* `mvn clean test` 运行单元测试，它的测试覆盖范围有限。
    * 单元测试没有额外的依赖且能在任何机器上运行
    * 单元测试不覆盖端到端的场景，如构建，合并和查询。
    * 单元测试只需几分钟即可完成。
* `dev-support/test_all_against_hdp_2_4_0_0_169.sh` 运行集成测试，有很好的覆盖率。
    * 集成测试 __最好运行在 Hadoop 沙箱上__。我们建议您在沙箱中检出一个代码副本，并在其中直接运行 test_all_against_hdp_2_2_4_2_2.sh。如果您不想在沙箱上添加代码，请参阅 __更多关于 V1.5 UT/IT 分离__ 的内容。
    * 正如名称所示，脚本仅适用于 hdp 2.2.4.2，但您可以从中理解集成测试是如何运行的。
    * 集成测试从生成随机数据开始，然后构建 cube、合并 cube，最后查询结果并与 H2 DB 进行比较。
    * 集成测试需要一到两个小时才能完成。
* `nohup dev-support/test_all_against_hdp_2_4_0_0_169.sh < /dev/null 2>&1 > nohup.out &` 以无人值守的方式运行集成测试。

## 测试 v1.3 及以下

* `mvn test` 运行单元测试，它的测试覆盖范围有限。
    * v1.3 及以下版本的特殊之处在于，hadoop / hbase 迷你集群用于覆盖单元测试中的查询。
* 运行以下命令来进行集成测试。
    * `mvn clean package -DskipTests`
    * `mvn test  -Dtest=org.apache.kylin.job.BuildCubeWithEngineTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`
    * `mvn test  -Dtest=org.apache.kylin.job.BuildIIWithEngineTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`
    * `mvn test  -fae -P sandbox`
    * `mvn test  -fae  -Dtest=org.apache.kylin.query.test.IIQueryTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`

## 更多关于 V1.5 UT/IT 分离

运行 `mvn verify -Dhdp.version=2.2.4.2-2` （假定您运行在沙箱上) 是运行完整的所有测试套件所需的全部内容。

它将按顺序执行以下步骤：
 
    1. Build Artifacts 
    2. Run all UTs (takes few minutes) 
    3. Provision cubes on the sandbox environment for IT uasge (takes 1~2 hours) 
    4. Run all ITs (takes few tens of minutes) 
    5. verify jar stuff 

如果您的代码改动很小那么只需要运行 UT，使用： 
`mvn test`
如果您的沙箱已经配置，并且您的代码更改不会影响沙箱配置的结果，（并且您不想等待数小时的配置），只需运行以下命令来单独运行 UT 和 IT： 
`mvn test`
`mvn failsafe:integration-test`

### Cube 配置

测试用的 cube 配置确实会运行 kylin cube 作业，以在沙箱中准备样例 cube。这些准备好的 cubes 将会被集成测试使用，当前准备步骤与 maven 预集成测试阶段绑定，且其包含运行 BuildCubeWithEngine（需要 HBase)，BuildCubeWithStream（需要 Kafka) 以及 BuildIIWithStream(需要 Kafka)。您可以在沙箱或进行开发的计算机中运行 mvn 命令。对于后者情况您需要在 __$KYLIN_HOME/examples/test_case_data/sandbox/kylin.properties__ 中设置 kylin.job.use-remote-cli=true。
尝试将 `-DfastBuildMode=true` 附加到 mvn verify 命令，通过跳过增量 Cubing 来加速配置。 

## 更多关于 v1.3 迷你集群

Kylin v1.3 (以下) 用于将尽可能多的单元测试用例从沙箱移动到 HBase 迷你集群，这样用户就可以在没有 hadoop 沙箱的情况下在本地轻松运行测试。在根 pom.xml 中创建了两个 maven profiles，“default”和“sandbox”。默认配置文件将启动 HBase 迷你集群以准备测试数据并运行单元测试（迷你集群不支持的测试用例已添加到“exclude”列表中）。如果您想继续使用 Sandbox 运行测试，只需运行 `mvn test -P sandbox`。

### 当使用 "default" 配置文件, Kylin 将会

* 启动 HBase 迷你集群并用动态的 HBase 配置更新 KylinConfig
* 创建 Kylin 源数据表并引入 6 个样例 cube 表
* 从本地的 tar 包导入 hbase 数据：`examples/test_case_data/minicluster/hbase-export.tar.gz` (hbase-export.tar.gz 将会在运行完成 BuildCubeWithEngineTest 后更新）
* 完成所有测试用例后，关闭迷你集群并清理 KylinConfig 缓存

### 为了确保迷你集群能成功运行, 您需要

* 确保正确设置了 JAVA_HOME 
