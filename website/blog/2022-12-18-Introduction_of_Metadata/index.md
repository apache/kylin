---
title: Introduction of Metadata(CN)
slug: introduction_of_metadata_cn
authors: pfzhan
tags: [metadata, kylin5]
hide_table_of_contents: false
date: 2022-12-18T17:00
---

:::tip Before your read
**Target Audience**
- Kylin 5.0 的开发者和用户

**What will you learn**
- 了解 Kylin 5.0 的元数据 Schema 设计和原理

💬 Kylin5 对[元数据的设计](https://github.com/apache/kylin/blob/doc5.0/website/blog/2022-12-18-Introduction_of_Metadata/protocol-buffer/metadata.proto)
做了比较大的调整，本文将针对这些调整做比较详细的介绍。
:::

<!--truncate-->

# Introduction of Metadata

Kylin5 对元数据的组织结构做了比较大的调整，本文将针对这些调整做比较详细的介绍。相比于 Kylin4，Kylin5 的元数据的一个显著特点是项目级隔离，也即每个项目彼此独立、互不干扰。本文会从项目开始，分别展开Table、Model、IndexPlan、Segment、Job 各个部分的内容。更宽泛地说，Kylin5 的元数据还包括元数据更新审计日志(AuditLog)、事务表(Epoch)、权限(ACL)、查询历史(Query History) 等内容。所有的这些内容都很重要，但作为入门级的介绍，本文不涉及更宽泛的内容，而是尽量把篇幅控制在元数据中最为基础的那部分，以帮助更多的开发者快速地了解和参与到 Kylin5 的研发。当然，作为一篇入门级介绍性文章，非研发人员阅读也能有所收获。接下来让我们切入主题。

## **Overview**

当启动一个 Kylin5 的实例在这个实例上进行了一些常规操作，创建项目、加载表、建模、编辑聚合组、构建索引等操作后，通过执行脚本 `{KYLIN_HOME}/metadata.sh backup` 可以得到类似于如下结构的元数据树状图。

```
.
├── UUID
├── _global
│   ├── project
│   │   ├── def.json
│   │   └── ssb.json
│   ├── resource_group
│   │   └── relation.json
│   ├── sys_acl
│   │   └── user
│   │       └── ADMIN
│   ├── user
│   │   └── ADMIN
│   └── user_group
│       ├── ALL_USERS
│       ├── ROLE_ADMIN
│       ├── ROLE_ANALYST
│       └── ROLE_MODELER
├── def
│   ├── dataflow
│   │   └── de52affe-f280-dcd1-be78-7865ff149669.json
│   ├── dataflow_details
│   │   └── de52affe-f280-dcd1-be78-7865ff149669
│   │       ├── 0b36355d-03df-8b02-aaf9-c2ab00e30456.json
│   │       ├── 743c5345-a3cd-9acf-2c90-c5bcec61c600.json
│   │       └── 7cbe4459-cbbd-8b8f-bb88-6a54c24d76ce.json
│   ├── execute
│   │   ├── 1c7dd6c1-6da5-72e6-a2fe-e87e5c764502-de52affe-f280-dcd1-be78-7865ff149669
│   │   ├── 383edce9-fad4-eba9-a498-0f289f1f1a79-de52affe-f280-dcd1-be78-7865ff149669
│   │   ├── 88400e7b-104a-9117-343d-cc3047b49983
│   │   ├── 95300d93-da1e-6505-b710-2771c724fa63-de52affe-f280-dcd1-be78-7865ff149669
│   │   ├── c11e4632-578b-1007-d3e6-65afca6f003c-de52affe-f280-dcd1-be78-7865ff149669
│   │   └── e1731c9e-d70b-e2eb-0fca-6be24751fb72
│   ├── index_plan
│   │   └── de52affe-f280-dcd1-be78-7865ff149669.json
│   ├── job_stats
│   │   └── 1671292800000.json
│   ├── model_desc
│   │   └── de52affe-f280-dcd1-be78-7865ff149669.json
│   ├── table
│   │   ├── SSB.CUSTOMER.json
│   │   └── SSB.LINEORDER.json
│   └── table_exd
│       ├── SSB.CUSTOMER.json
│       └── SSB.LINEORDER.json
├── ssb
│   ├── dataflow
│   │   ├── 407f1b4e-e5d7-6c1d-3697-a3deaffd0f6b.json
│   │   └── 91b2007b-112f-f98e-b967-c2fe26c6761c.json
│   ├── dataflow_details
│   │   └── 91b2007b-112f-f98e-b967-c2fe26c6761c
│   │       └── 19211ed7-9c05-cc8e-9d05-7b64d0b90cf8.json
│   ├── execute
│   │   ├── 5dde1d4b-f60b-7601-bdda-4d20493b324d-91b2007b-112f-f98e-b967-c2fe26c6761c
│   │   ├── add2d7c6-4f51-1144-b05f-a28da0d42e1d
│   │   └── c144cb8f-28c8-833e-3990-59e51bd3f7f8
│   ├── index_plan
│   │   ├── 407f1b4e-e5d7-6c1d-3697-a3deaffd0f6b.json
│   │   └── 91b2007b-112f-f98e-b967-c2fe26c6761c.json
│   ├── job_stats
│   │   └── 1671292800000.json
│   ├── model_desc
│   │   ├── 407f1b4e-e5d7-6c1d-3697-a3deaffd0f6b.json
│   │   └── 91b2007b-112f-f98e-b967-c2fe26c6761c.json
│   ├── table
│   │   ├── SSB.DATES.json
│   │   └── SSB.LINEORDER.json
│   └── table_exd
│       ├── SSB.DATES.json
│       └── SSB.LINEORDER.json
```

从上面的这个树形结构很容易看出 Kylin5 的元数据是项目级隔离的，_global 这个项目相对特殊，它用来存储系统级别的信息如当前实例的项目元数据、ACL 权限相关的元数据、用户以及用户组等信息。权限相关的部分略去不谈，将重点放在单个项目的元数据组织结构上。这份元数据只包括两个项目 ssb 和 def，它们的组织结构完全相同，接下来将一一展开介绍，这里先简要地说明每个目录的作用。

- table: 记录该项目加载的所有表的元数据信息
- table_exd: 记录 table 目录下表对应的扩展描述性信息
- model_desc: 记录模型的元数据信息
- index_plan: 记录索引相关的元数据信息
- dataflow: 记录 segment 相关的元数据信息
- dataflow_details: 这是个目录，里面每个文件记录的是已构建索引的描述性信息
- execute: 记录构建任务相关的元数据信息
- job_status: 记录构建任务执行状态的元数据信息

将 execute 和 job_status 对应的信息排除在外，那么其它的部分是组成一个项目最为核心的元数据信息。上面这个树状图是备份元数据之后的结果，实际上它们都存在同一张元数据表中，所有的元数据都是一条条的表记录，记录的绝对路径就是它在原数据表中的 meta_key，比如，/ssb/table/SSB.DATES.json 这个绝对路径就代表了ssb 项目加载的表 SSB.DATES。

## **Table Description**

表的描述性文件信息包括 table 和 table_exd 两个文件夹下的内容。table/SSB.DATES.json 是从数据源加载到 Kylin5 系统中生成的表基础描述性信息，而 table_exd/SSB.DATES.json 则是表的扩展信息。随着 Kylin5 功能的扩展，它可能越来越丰富。就目前来说，扩展信息包括表采样信息和查询命中次数信息。

在表的描述性信息中，大部分属性很清晰，这里主要对几个解释一下含义。

- `source_type` 表的源信息，ISourceAware 这个类中定义了一些常规的数据源的类型。
- `table_type` 表的类型，来源于表或者视图。
- `transactional` 事务表标志。
- `increment_loading、top、rangePartition` 已经属于废弃字段。
- `query_hit_count` 已移到表扩展信息中。
- 关于表的描述性信息中还有保留了一些快照相关的信息，这是不太合理的，因为快照可能在每次构建时候都自动更新。

值得注意的是，这里的表的描述性信息是项目级别的，而实际模型在引用到这些元数据的时候会利用模型上添加的可计算列信息加以扩展，从而使得表上的列会增加，但这部分内容是在内存中存在的，不会被保存到元数据库中。关于这部分内容，接下来的模型部分会给予进一步说明。

## **Model Description**

模型是 Kylin5 的一个核心概念，可以认为它是一系列相关业务的抽象，在这个抽象中包含了一个或者多个业务模式 (对应到索引)。模型包含的概念比较多，如维度、度量、可计算列、普通列、表的关联关系、事实表、维表、星型模型、雪花模型、星座模型（暂不支持）等，大部分概念在维度建模理论中都有论述，本文不再做说明。这里仅介绍 Kylin5 中特有的概念：**普通列** 和**可计算列**。

先说**可计算列**(ComputedColumn)，借用 Kylin5 手册中的一段话来说明可计算列。

> 可计算列是为了充分利用 Kylin 的预计算能力而在模型中预先定义的数据列。通过将相对复杂的在线计算转换成基于可计算列的预计算，查询的性能将会得到大幅提升。此外，可计算列支持将数据的转换、重定义等操作预先定义在模型中，增强数据语义层。通过定义可计算列，用户可以重用已有的业务逻辑代码。
> 

在模型定义可计算列时，会同时往普通列中添加一个同名的列，目前可计算列只能定义在事实表上。可计算列之后就可以在维度和度量定义时使用。前文讲 Table 这部分内容的时候提到，模型使用的表扩展了可计算列，它是通过模型的 getExtendedTables 方法在使用 table 之前扩展进来的，所以模型使用的是增强了语义信息的表。 

**普通列**(NamedColumn) 的来源有两个，一是前文已经说明可计算列的定义会同时增加一个普通列，二是来源于建模时添加的事实表和所有被关联上的维表。普通列有个属性 status 用于标记这个列是维度列(DIMENSION)、被删除的列(TOMB)、还是仅仅是个普通列(EXIST)。当和可计算列同名的列的 status 属性是 DIMENSION 则表明可计算列被定义成了维度。

### **Significant Change**

Kylin5 中定义了**模型的重大变更**，以确定是否需要执行一系列的后续操作。重大变更会删除无价值的维度和度量，可能会触发 Segment 重新构建，而且模型的 semantic_version 属性在每次发生重大变更后都会自增。以下任意一种情况发生，都会被认定为模型已经发生重大变更。

- 分区列、多级分区列的变化
- 事实表变更
- 模型中表的关联关系变更
- 模型的过滤条件发生变化
- 模型关联的维表是否预计算属性被更改

### **Broken Model**

由于 model_desc、index_plan、dataflow 之间存在一一对应的关系，因此这三者中的任意一个遭到破坏，模型都会以 broken 状态展示出来。损坏的模型需要用户干预去修复。模型什么时候会损坏？一般来说，模型损坏来源于重载表操作，当数据源中表的列被删除、或者表的列数据类型产生无法兼容的变更、表被删除等情况发生时，重载表有可能导致模型损坏。**因此，在重载表或者做一些相对比较危险性的操作时，记得要先备份元数据。**

模型损坏时会触发哪些操作？优化建议全部清空、dataflow 上会记录失败的原因。失败原因分为三类：由 EVENT 触发的、由 SCHEMA 触发的、其他未知问题触发的 (NULL)。

- EVENT 导致的失败
- SCHEMA 导致的失败会删除所有已经构建好的 segment。
- NULL 导致的失败

## **IndexPlan**

IndexPlan 是 Kylin5 中用来组织 Index/Layout 的元数据，它里面包含两个最重要的信息 RuleBasedIndex 和 Indexes，前者用于管理聚合组索引，后者用于管理被物化的索引。在介绍这两个概念之前，我们先介绍一下 Index 和 Layout 这组概念。

### **Index & Layout**

Index 是一个集合概念(后面翻译为索引)，它将一类 Layout 管理在一起，这些 Layout 需要满足维度和度量的元素集合相同，但它们的排列不同，或者它们的 shardByColumn 不一样。Layout 时这个集合中的一个具体的排列。

- Index 的 id 是 10000 的整数倍
- Layout 与 Index 之间的关系: 参考下面的例子会比较清楚
- 只要不引起歧义，可使用 Index 来代指一个具体的 Layout
- 同样的 Layout 被删除后如果再生成出来，那么它的 ID 是全新的

```
假设模型有3个维度分别是{1, 2, 3} ，2个度量分别是{100000, 100001}，
当使用全部维度和度量时，Index 的 Bitset 包含{1, 2, 3, 100000, 100001}；
但是生成的 Layout 可以有很多个，比如：
 {col_order = [1, 2, 3, 100000, 100001] , shard_by_column = []},
 {col_order = [1, 2, 3, 100000, 100001] , shard_by_column = [1]},
 {col_order = [2, 1, 3, 100000, 100001] , shard_by_column = []},
 {col_order = [3, 1, 2, 100000, 100001] , shard_by_column = [2]}
注: col_order 是维度的一个排列再加上度量。shard_by_column 影响查询效率。
    Kylin5在大部分情况下使用的是id值，维度、度量、Layout都是这样。
```

当前 Kylin5 设计中，Layout 定义了两个属性 manual 和 auto，manual 用来标记索引是用户自定义的，auto 用来标记索引是自动化程序或者脚本生成的，开发者可自行扩展。定义两个属性是为了更好的可扩展性，比如索引先由程序生成出来，之后用户又编辑了聚合组也生成了一个相同的索引，但又不希望这个信息被覆盖掉。按照这两个属性可以将索引划分为用户自定义索引和自动化索引。一般来说程序自动生成的索引比较灵活，也能够清晰的看到索引的组成部分(元数据被物化下来了，或许在不久的将来 Kylin5 将所有的索引都物化下来也未可知)，开发者可自行扩展。

按照索引是否预计算聚合将索引分为聚合索引和明细索引。一个不太优雅且通俗易懂的说法，聚合索引就是带度量的索引，明细索引相反。此外，Layout 还定义了一个属性 base 用于标注索引是否是基础索引，基础索引能够尽最大能力避免查询下压到其他计算引擎，比如 Kylin5 自带的 Spark 下压引擎，用户可自行扩展其他计算引擎。

### **RuleBasedIndex**

RuleBasedIndex 里面包含多个聚合组(NAggregationGroup)，每个聚合组可以定义自己的生成规则，具体包括必须维度、联合维度、层级维度以及最大维度组合数。RuleBasedIndex 也可以定义一个模型级别的最大维度组合数。当删除 RuleBasedIndex 中的索引时，索引会被加入到 `layout_black_list` 中来保证编辑聚合组不至于导致索引出现 ID 错乱的情况。如果出现模型重大变更 `layout_black_list` 会被清空，整个聚合组会重新生成索引，需要重刷 Segment 数据。

```json
"rule_based_index" : {
  "dimensions" : [ 0, 10, 16, 26, 17, 19 ],
  "measures" : [ 100000, 100001, 100002, 100003, 100004 ],
  "global_dim_cap" : null,
  "aggregation_groups" : [ {
    "includes" : [ 0, 10, 16, 26, 17, 19 ],
    "measures" : [ 100000, 100001, 100002, 100003, 100004 ],
    "select_rule" : {
      "hierarchy_dims" : [ ],
      "mandatory_dims" : [ 10, 0, 16 ],
      "joint_dims" : [ [ 17, 19 ] ]
    },
    "index_range" : "EMPTY"
  } ],
  "layout_id_mapping" : [ 10001, 20001, 30001, 40001 ],
  "parent_forward" : 3,
  "index_start_id" : 10000,
  "last_modify_time" : 1671335454291,
  "layout_black_list" : [ ],
  "scheduler_version" : 2,
  "index_update_enabled" : true, /* streaming 相关，可以忽略 */
  "base_layout_enabled" : true  /* 是否生成包含所有聚合组维度度量的大索引 */
}
```

### **Indexes**

Indexes 属性用于管理用户自定义明细索引、基础明细索引、基础聚合索引以及用户通过扩展 Kylin 自动生成的索引。这类索引区别于聚合组生成的索引的显著特点是所有信息一目了然。对于开发 Kylin 功能以及排查有些查询问题特别方便。需要注意的是，基础聚合索引和基础明细索引会随着编辑模型添加维度、度量重新生成，之后原来的基础索引就变成了普通索引并且会变为锁定状态，在新的基础索引没有构建好之前仍然可以供用户查询。构建好之后，锁定状态的索引不会自动删除，需要用户主动触发删除。

```json
"indexes" : [ {
  "id" : 0,
  "dimensions" : [ 0, 10, 16, 17, 19, 22, 26 ],
  "measures" : [ 100000, 100001, 100002, 100003, 100004 ],
  "layouts" : [ {
    "id" : 1,
    "name" : null,
    "owner" : null,
    "col_order" : [ 0, 10, 16, 17, 19, 22, 26, 100000, 100001, 100002, 100003, 100004 ],
    "shard_by_columns" : [ ],
    "partition_by_columns" : [ ],
    "sort_by_columns" : [ ],
    "storage_type" : 20,
    "update_time" : 1671335046320,
    "manual" : false, /* 聚合组定义的索引 */
    "auto" : false,  /* 自动化程序生成的索引 */
    "base" : true,  /* 基础索引 */
    "draft_version" : null, /* 废弃属性 */
    "index_range" : null    /* streaming 相关暂时可忽略 */
  } ],
  "next_layout_offset" : 2
}]
```

最后介绍 IndexPlan 的其他重要属性来结束这部分内容。

- `next_aggregation_index_id` 记录新的聚合索引可以分配的 ID；
- `next_table_index_id` 记录新的明细索引可以分配的 ID；
- `approved_additional_recs、approved_removal_recs` 非开源功能，无需关注；
- `retention_range、engine_type`、 废弃属性

## **Segment**

这一部分是对 Segment 数据存储的描述性性信息，包括 dataflow 和 dataflow_details 两部分。其中，dataflow 用于存储 segment自身的描述性信息，而 dataflow_details 则存储的是每个 segment 已经构建的索引的一些描述性信息。

### **Dataflow**

这部分元数据主要描述了Segment的总体信息，分类说明一些重要的信息。

status 状态信息包括：ONLINE、OFFLINE、WARNING。ONLINE 就是模型在线可供查询，OFFLINE 反之。模型 OFFLINE 的场景包括：

```
刚新建的无分区列的模型(构建好会自动 ONLINE)
模型没有任何 segment，模型自动 OFFLINE
克隆出来的新模型默认 OFFLINE
用户主动下线模型
```

值得关注的是 WARNING 状态的模型，它表明模型存在异构，这是因为 Kylin5 允许对一个模型并发构建以及在线的 Schema 变更。与前面模型部分已经提到重大变更不同，这里的变更对已有的索引和 Segment 影响不大，比如：删掉了一些索引、增加了一些维度度量以及索引、添加了一些可计算列等。Segment 中的索引异构分几种情况：

- 对部分 segment 删除索引导致的；
- 新增索引导致的；
- segment 中构建的索引依赖了不同的平表

segments 这个属性刻画了模型中存在多少 segment，每个 segment 是增量构建还是全量构建、构建的时间范围多大、segment 的 min/max 统计信息。除此之外，dataflow 上还记录了一些查询统计信息，设计上的缺陷，不应该把查询统计信息和描述性信息耦合在一起。

### **DataflowDetails**

这一部分主要用于记录每个 segment 构建了哪些索引以及每个索引构建好的数据的一些统计信息。当查询到具体的 segment 中的索引时，查询结果会统计并展示这些信息的汇总结果。dataflow_details 这个目录中的数据在没有构建索引的情况下是不存在的，但是 dataflow 里面的信息则不一样，即使没有添加任何segment，也会有一个与模型对应的文件，只是里面的属性 segments 为空数组。

```json
"layout_instances" : [{
  "layout_id" : 1,
  "build_job_id" : "xxxxx",
  "rows" : 4942,
  "byte_size" : 138432,
  "file_count" : 2,
  "source_rows" : 18416,
  "source_byte_size" : 0,
  "partition_num" : 1,
  "partition_values" : [ ],
  "is_ready" : false,
  "create_time" : 1671335217776,
  "multi_partition" : [ ]
}]
```

## **Task & Job**

Task 这一部分与构建任务相关，包括 execute 和 job_status 两部分。相比于以上部分，这部分内容更多的被用在构建任务报错后定位问题。这边不做详细讨论，有兴趣可以直接参考代码理解这些内容。重点提一下，execute 下的 job 的 uuid 可能很长，那是因为它后缀了模型的 uuid，起作用是在构建时避免项目锁的抢占，它能够提升整个系统的稳定性和并发构建的能力。

## **Brief Summary**

最后介绍一下单个项目的描述性信息，它里面的属性和项目级设置相关，这里挑一些重要的作介绍。default_database 项目默认数据库，通过设置默认数据用户可以在查询的时候对默认数据库中的表不用加上数据库前缀。override_kylin_properties 记录项目覆盖的配置信息，目前 Kylin5 提供的查询相关的配置，基本上都可以定义到项目级别。segment_config 中的配置在 Segment 合并时使用。

总而言之，Kylin5 的新一代元数据设计旨在提高系统的易用性、可扩展性，比如：IndexPlan 的抽象、Index & Layout 的设计、索引的属性 manual & auto 的设计等。这些都开发者进一步探索留下了广阔的空间。除此之外，Kylin5 也提供了诸多新颖的特性，如：

- 可计算列丰富了预计算能力
- AuditLog & Epoch 提供了全新的元数据同步机制
- 灵活的语义变更，在大多数场景下不需要重建模型也能够更灵活的应对业务变化
- 明细索引，赋予了从 Valuable 的数据中下钻以及上卷的能力
- 更灵活的 Runtime Join 从而更好的发挥预计算与实时计算各自的长处

本文主要是一篇介绍性的文章，希望能为您切入 Kylin5 提供些许帮助，接下来我们将提供更多关于 Kylin5 新特性设计原理的文章，期待大家一起加入到 Kylin5 的研发和试用。
