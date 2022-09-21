---
title: Metastore Design of Kylin 5
language: en
sidebar_label: Metastore Design of Kylin 5
pagination_label: Metastore Design of Kylin 5
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/how_to_understand_kylin_design
pagination_next: null
showLastUpdateAuthor: true
showLastUpdateTime: true
keywords:
  - dev-design
draft: false
last_update:
  date: 09/16/2022
  author: Xiaoxiang Yu
---

### Target Audience

这篇文档是为有以下需求的用户和开发者而准备的:
1. 对 Kylin 5.0 元数据存储, 元数据缓存, 以及节点间元数据同步机制感兴趣的用户和开发者.
2. 在二次开发过程中, 想了解对 Kylin 5.0 进行元数据读写操作的最佳实践和注意事项的开发者.
3. 想对 Kylin 5.0 的元数据进行升级改造的开发者.

### Terminology

#### Core Class and Interface

| Class                   | Comment                                            |
|-------------------------|----------------------------------------------------|
| ResourceStore           | 用于管理内存中对元数据的操作                                     |
| InMemResourceStore      | ResourceStore 的实现类, 用于绝大部分情况                       |
| ThreadViewResourceStore | ResourceStore 的实现类, 作为一个沙盒式的 ResourceStore, 在事务中使用 |
| MetadataStore           | 用于管理元数据持久化的操作                                      |
| AuditLogStore           | 用于节点间元数据同步, 以及诊断元数据异常情况                            |
| Epoch                   | 用于保证同时只有一个进程对指定项目下的元数据进行修改操作, 或者提交作业               |
| EpochStore              | 用于持久化 Epoch                                        |


### Question and Answer

#### 1. Why Kylin 5.0 need transaction(of metadata) ?
因为用户操作可能同时操作多个元数据, 这些元数据变更必须保持一致性, 要么同时变更成功要么同时变更失败, 不允许出现中间状态. 
例如, 如果用户修改 DataModel 的维度和度量, IndexPlan 也需要随之同时变更, 两者必须保持一致性.

#### 2. How transaction was implemented？
检查 [元数据更新流程图](#metadata_write)

#### 3. How do meta was synced in Kylin Cluster？
对于指定项目, Kylin 节点要获取 Epoch 才能对元数据进行写操作, 所以对于这个项目下面元数据, 没有获取 Epoch 的 Kylin 节点
将作为 Follower 通过 AuditLogReplayWorker 获取元数据更新.

Follower 同步元数据变更，通过两个方式，代码在 AuditLogReplayWorker
1. 第一个是 Follower 自己定期调度同步任务，默认间隔是 5s；
2. 第二个方式是元数据变更的节点发送 AuditLogBroadcastEventNotifier 告知所有 Follower, Follower 主动 replay

按照设计，Follower 元数据的 delay 在 1-2s 左右(被动广播同步)，最多 5s(主动定期同步).

#### 4. How ResourceStore was inited when Kylin started?
todo

#### 5. As a kylin developer, how should I write my code to update metadata?


1. 使用 `@org.apache.kylin.rest.aspect.Transaction` 对你的业务方法进行注解, 这个在你的方法比较简短(轻量)的情况比较推荐
    ```java
    class JobService{
        
      @Transaction(project = 0)
      public ExecutableResponse manageJob(String project, ExecutableResponse job, String action) throws IOException {
          Preconditions.checkNotNull(project);
          Preconditions.checkNotNull(job);
          Preconditions.checkArgument(!StringUtils.isBlank(action));
      
          if (JobActionEnum.DISCARD == JobActionEnum.valueOf(action)) {
              return job;
          }
      
          updateJobStatus(job.getId(), project, action);
          return getJobInstance(job.getId());
      }
    }
    ```
2. 使用 `EnhancedUnitOfWork.doInTransactionWithCheckAndRetry` 来包含你的业务代码(元数据修改代码)

```java
class SomeService {
    public void renameDataModel() {
        // some codes not in transaction
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            // 1. Get XXXManager, such as NDataModelManager
            KylinConfig config = KylinConfig.getInstanceFromEnv(); // thread local KylinConfig was created in startTransaction
            NDataModelManager manager = config.getManager(project, NDataModelManager.class);
            // 2. Get RootPersistentEntity by XXXManager and Update XXX
            NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
            checkAliasExist(modelId, newAlias, project);
            nDataModel.setAlias(newAlias);
            NDataModel modelUpdate = modelManager.copyForWrite(nDataModel);
            // 3. Call updateXXX method of XXXManager
            modelManager.updateDataModelDesc(modelUpdate);
        }, project);
        // some more codes not in transaction
    }
}
```

:::info 元数据操作须知
1. KylinConfig 和 XXXManager(例如:NDataModelManager) 需要在事务内获取, 
原因在于事务开启时准备了一个事务专用的单独的 KylinConfig, 单独的 KylinConfig 绑定了事务需要的 ResourceStore
2. 开启多线程修改元数据需要注意, 原因在于新的线程并不能获取事务开启时准备的 KylinConfig, 而是全局的 KylinConfig
3. 两个事务写的时候别复用同一个对象, 以避免元数据更新时, MVCC 检查失败
::: 

#### 6. What is Epoch and how do Epoch works? 

1. 什么是 Epoch?
    
  `Epoch` 是全局级别的, 项目粒度的元数据写锁, 用于确保同一时刻只有一个进程会修改指定项目下的元数据.

2. 什么样的进程可以获取 Epoch?

  Job 和 All, Query 不可以获得 Epoch, Query 节点被设计为不需要提交任务或者是修改元数据.

3. Epoch 如何管理?
   

相关类
- EpochOrchestrator
- EpochOrchestrator.EpochChecker
- EpochOrchestrator.EpochRenewer
- EpochManager
- EpochChangedListener

### <span id="metadata_write">Diagram of write a piece of meta </span>

#### About Activity Diagram
1. 元数据事务的相关代码在 `UnitOfWork`, 本活动图是对代码的说明;
2. `Epoch` 是全局级别的, 项目粒度的元数据写锁, 用于确保同一时刻只有一个进程会修改指定项目下的元数据;
3. 为了避免多线程同时变更元数据, 所以获得 Epoch 的进程, 还需要进行一个进程内的写锁的锁定, 以确保获得 `Epoch` 的进程对元数据的变更, 是串行的;
4. 调用 `endTransaction` 方法前, 元数据变更会发生在一个沙盒模式的 `ResourceStore`;
5. 在 `endTransaction` 方法, 会将元数据变更持久化到 `Metastore`(RDBMS) 和 `AuditLogStore`(RDBMS), 这里使用 RDBMS 的事务保证元数据操作的一致性;
6. 关于事物的异常和回滚, 在 `process` 方法发生的异常因为元数据变更是发生在沙盒的, 所以直接舍弃沙盒即可; 
在 `endTransaction` 发生的异常, 由 RDBMS 事务保证对 Metastore 和 AudiLogStore 操作的原子性;
7. 其他 Kylin 进程会定期获取 `AuditLogStore` 的变更, 重放到自己的 `ResourceStore`, 来保持节点间的元数据的一致性.

![](diagram/activity_diagram/write_and_read_persistent_entity_cn.png)

### <span id="class_metastore">Class Diagram of Metastore</span>
![](diagram/class_diagram/metastore_resource_store.png)
