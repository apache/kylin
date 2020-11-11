---
layout: docs-cn
title:  样例数据集
categories: howto
permalink: /cn/docs/howto/sample_dataset.html
---

# 样例数据集

Kylin 的二进制包中包含了一份样例数据集，共计 5 张表，其中事实表有 10000 条数据。用户可以在 Kylin 部署完成后，利用样例数据集进行测试。用户可通过执行脚本方式，将 Kylin 中自带的样例数据导入至 Hive。

### 将样例数据集导入至 Hive

导入样例数据集的可执行脚本为 **sample.sh** ，其默认存放路径为系统安装目录下的 **/bin** 目录：

```sh
$KYLIN_HOME/bin/sample.sh
```

脚本执行成功之后，可在服务器终端执行 **hive** 命令行，进入 hive，然后执行查询语句验证导入正常：

```she
hive
```

系统默认将 5 张表导入 Hive 的 `default` 数据库中，用户可以检查导入 Hive 的表清单或查询具体表：

```sql
hive> use default;
hive> show tables;
hive> select count(*) from kylin_sales;
```

> 提示：如果需要将表导入至 Hive 中指定的数据库，您可以修改 Kylin 配置文件 `$KYLIN_HOME/conf/kylin.properties` 中的配置项 `kylin.source.hive.database-for-flat-table` 至指定的 Hive 数据库。

### 数据表介绍

本产品支持星型数据模型和雪花模型。本文中用到的样例数据集是一个规范的雪花模型结构，它总共包含了 5 个数据表：

- **KYLIN_SALES**

  事实表，保存了销售订单的明细信息，每一行对应着一笔交易订单。交易记录包含了卖家、商品分类、订单金额、商品数量等信息，

- **KYLIN_CATEGORY_GROUPINGS**

  维度表，保存了商品分类的详细介绍，例如商品分类名称等。

- **KYLIN_CAL_DT**

  维度表，保存了时间的扩展信息。如单个日期所在的年始、月始、周始、年份、月份等。

- **KYLIN_ACCOUNT**

  维度表，用户账户表，每行是一个用户。用户在事实表中可以是买方(Buyer)或者卖方(Seller)。通过 ACCOUNT_ID 链接到 **KYLIN_SALES** 的 BUYER_ID 或者 SELLER_ID 上。

- **KYLIN_COUNTRY**

  维度表，用户所在的国家表，链接到 **KYLIN_ACCOUNT**。

这5张表一起构成了整个雪花模型的结构，下图是实例-关系（ER）图：

![样例数据表](/images/SampleDataset/dataset.png)

### 数据表与关系

通过脚本 `sample.sh`  生成的 Hive 表中包含的列较多，下面以样例项目 `learn_kylin` 中模型 `kylin_sales_models` 中被定义为维度的列为主，介绍一些主要的列。

| 表                       | 字段                | 意义           |
| :----------------------- | :------------------ | :------------- |
| KYLIN_SALES              | TRANS_ID             | 订单 ID      |
| KYLIN_SALES              | PART_DT             | 订单日期       |
| KYLIN_SALES              | LEAF_CATEG_ID       | 商品分类 ID    |
| KYLIN_SALES              | LSTG_SITE_ID       | 网站 ID    |
| KYLIN_SALES              | SELLER_ID           | 卖家 ID        |
| KYLIN_SALES              | BUYER_ID            | 买家 ID        |
| KYLIN_SALES              | PRICE               | 订单金额       |
| KYLIN_SALES              | ITEM_COUNT          | 购买商品个数   |
| KYLIN_SALES              | LSTG_FORMAT_NAME    | 订单交易类型   |
| KYLIN_SALES              | OPS_USER_ID          | 系统用户 ID  |
| KYLIN_SALES              | OPS_REGION    | 系统用户地区   |
| KYLIN_CATEGORY_GROUPINGS | USER_DEFINED_FIELD1 | 用户定义字段 1 |
| KYLIN_CATEGORY_GROUPINGS | USER_DEFINED_FIELD3 | 用户定义字段 3 |
| KYLIN_CATEGORY_GROUPINGS | UPD_DATE            | 更新日期       |
| KYLIN_CATEGORY_GROUPINGS | UPD_USER            | 更新负责人     |
| KYLIN_CATEGORY_GROUPINGS | META_CATEG_NAME     | 一级分类       |
| KYLIN_CATEGORY_GROUPINGS | CATEG_LVL2_NAME     | 二级分类       |
| KYLIN_CATEGORY_GROUPINGS | CATEG_LVL3_NAME     | 三级分类       |
| KYLIN_CAL_DT             | CAL_DT              | 日期           |
| KYLIN_CAL_DT             | WEEK_BEG_DT         | 周始日期       |
| KYLIN_CAL_DT             | MONTH_BEG_DT        | 月始日期       |
| KYLIN_CAL_DT             | YEAR_BEG_DT         | 年始日期       |
| KYLIN_ACCOUNT            | ACCOUNT_ID          | 用户账户 ID    |
| KYLIN_ACCOUNT            | ACCOUNT_COUNTRY     | 账户所在国家 ID |
| KYLIN_ACCOUNT            | ACCOUNT_BUYER_LEVEL          | 买家账户等级 |
| KYLIN_ACCOUNT            | ACCOUNT_SELLER_LEVEL     | 卖家账户等级 |
| KYLIN_ACCOUNT            | ACCOUNT_CONTACT     | 账户联系方式 |
| KYLIN_COUNTRY            | COUNTRY             | 国家 ID        |
| KYLIN_COUNTRY            | NAME                | 国家名称       |