---
layout: docs15-cn
title:  Kylin Cube 创建教程
categories: 教程
permalink: /cn/docs15/tutorial/create_cube.html
version: v1.2
since: v0.7.1
---
  
  
### I. 新建一个项目
1. 由顶部菜单栏进入`Query`页面，然后点击`Manage Projects`。

   ![](/images/Kylin-Cube-Creation-Tutorial/1 manage-prject.png)

2. 点击`+ Project`按钮添加一个新的项目。

   ![](/images/Kylin-Cube-Creation-Tutorial/2 %2Bproject.png)

3. 填写下列表单并点击`submit`按钮提交请求。

   ![](/images/Kylin-Cube-Creation-Tutorial/3 new-project.png)

4. 成功后，底部会显示通知。

   ![](/images/Kylin-Cube-Creation-Tutorial/3.1 pj-created.png)

### II. 同步一张表
1. 在顶部菜单栏点击`Tables`，然后点击`+ Sync`按钮加载hive表元数据。

   ![](/images/Kylin-Cube-Creation-Tutorial/4 %2Btable.png)

2. 输入表名并点击`Sync`按钮提交请求。

   ![](/images/Kylin-Cube-Creation-Tutorial/5 hive-table.png)

### III. 新建一个cube
首先，在顶部菜单栏点击`Cubes`。然后点击`+Cube`按钮进入cube designer页面。

![](/images/Kylin-Cube-Creation-Tutorial/6 %2Bcube.png)

**步骤1. Cube信息**

填写cube基本信息。点击`Next`进入下一步。

你可以使用字母、数字和“_”来为你的cube命名（注意名字中不能使用空格）。

![](/images/Kylin-Cube-Creation-Tutorial/7 cube-info.png)

**步骤2. 维度**

1. 建立事实表。

    ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-factable.png)

2. 点击`+Dimension`按钮添加一个新的维度。

    ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-%2Bdim.png)

3. 可以选择不同类型的维度加入一个cube。我们在这里列出其中一部分供你参考。

    * 从事实表获取维度。
          ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-typeA.png)

    * 从查找表获取维度。
        ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-typeB-1.png)

        ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-typeB-2.png)
   
    * 从有分级结构的查找表获取维度。
          ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-typeC.png)

    * 从有衍生维度(derived dimensions)的查找表获取维度。
          ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-typeD.png)

4. 用户可以在保存维度后进行编辑。
   ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-edit.png)

**步骤3. 度量**

1. 点击`+Measure`按钮添加一个新的度量。
   ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-%2Bmeas.png)

2. 根据它的表达式共有5种不同类型的度量：`SUM`、`MAX`、`MIN`、`COUNT`和`COUNT_DISTINCT`。请谨慎选择返回类型，它与`COUNT(DISTINCT)`的误差率相关。
   * SUM

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-sum.png)

   * MIN

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-min.png)

   * MAX

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-max.png)

   * COUNT

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-count.png)

   * DISTINCT_COUNT

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-distinct.png)

**步骤4. 过滤器**

这一步骤是可选的。你可以使用`SQL`格式添加一些条件过滤器。

![](/images/Kylin-Cube-Creation-Tutorial/10 filter.png)

**步骤5. 更新设置**

这一步骤是为增量构建cube而设计的。

![](/images/Kylin-Cube-Creation-Tutorial/11 refresh-setting1.png)

选择分区类型、分区列和开始日期。

![](/images/Kylin-Cube-Creation-Tutorial/11 refresh-setting2.png)

**步骤6. 高级设置**

![](/images/Kylin-Cube-Creation-Tutorial/12 advanced.png)

**步骤7. 概览 & 保存**

你可以概览你的cube并返回之前的步骤进行修改。点击`Save`按钮完成cube创建。

![](/images/Kylin-Cube-Creation-Tutorial/13 overview.png)
