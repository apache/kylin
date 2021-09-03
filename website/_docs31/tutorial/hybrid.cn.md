---
layout: docs31-cn
title:  Hybrid 模型
categories: 教程
permalink: /cn/docs31/tutorial/hybrid.html
version: v1.2
since: v2.5.0
---

本教材将会指导您创建一个 Hybrid 模型。 关于 Hybrid 的概念，请参考[这篇博客](http://kylin.apache.org/blog/2015/09/25/hybrid-model/)。

### I. 创建 Hybrid 模型
一个 Hybrid 模型可以包含多个 cube。

1. 点击顶部的 `Model`，然后点击 `Models` 标签。点击 `+New` 按钮，在下拉框中选择 `New Hybrid`。

    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/1 +hybrid.png)

2. 输入 Hybrid 的名字，然后选择包含您想要查询的 cubes 的模型，然后勾选 cube 名称前的单选框，点击 > 按钮来将 cube(s) 添加到 Hybrid。

    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/2 hybrid-name.png)
    
*注意：如果您想要选择另一个 model，您应该移除所有已选择的 cubes。* 

3. 点击 `Submit` 然后选择 `Yes` 来保存 Hybrid 模型。创建完成后，Hybrid 模型就会出现在左边的 `Hybrids` 列表中。
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/3 hybrid-created.png)

### II. 更新 Hybrid 模型
1. 点击 Hybrid 名称，然后点击 `Edit` 按钮。然后您就可以通过添加或删除 cube(s) 的方式来更新 Hybrid。 
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/4 edit-hybrid.png)

2. 点击 `Submit` 然后选择 `Yes` 来保存 Hybrid 模型。

现在您只能通过点击 `Edit` 按钮来查看 Hybrid 详情。

### III. 删除 Hybrid 模型
1. 将鼠标放在 Hybrid 名称上，然后点击 `Action` 按钮，在下拉框中选择 `Drop`。然后确认删除窗口将会弹出。 

2. 点击 `Yes` 将 Hybrid 模型删除。 

### IV. 运行查询
Hybrid 模型创建成功后，您可以直接进行查询。 因为 hybrid 比 cube 有更高优先级，因此可以命中 cube 的查询会优先被 hybrid 执行，然后再转交给 cube。

点击顶部的 `Insight`，然后输入您的 SQL 语句。
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/5 sql-statement.png)

*请注意, Hybrid model 不适合 "bitmap" 类型的 count distinct 跨 cube 的二次合并，请务必在查询中带上日期维度. *