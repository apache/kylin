---
layout: docs40-cn
title:  "样例 Cube 快速入门"
categories: tutorial
permalink: /cn/docs40/tutorial/kylin_sample.html
---

Kylin 提供了一个创建样例 Cube 脚本；脚本会创建五个样例 Hive 表:

1. 运行 `${KYLIN_HOME}/bin/sample.sh`；重启 Kylin 服务器刷新缓存;
2. 用默认的用户名和密码 ADMIN/KYLIN 登陆 Kylin 网站，选择 project 下拉框（左上角）中的 `learn_kylin` 工程;
3. 选择名为 `kylin_sales_cube` 的样例 Cube，点击 "Actions" -> "Build"，选择一个在 2014-01-01 之后的日期（覆盖所有的 10000 样例记录);
4. 点击 "Monitor" 标签，查看 build 进度直至 100%;
5. 点击 "Insight" 标签，执行 SQLs，例如:

```
select part_dt, sum(price) as total_sold, count(distinct seller_id) as sellers from kylin_sales group by part_dt order by part_dt
```

 6.您可以验证查询结果且与 Hive 的响应时间进行比较;
 
## 下一步干什么

您可以通过接下来的教程用同一张表创建另一个 Cube。
