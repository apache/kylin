---
layout: docs30-cn
title:  "Python 客户端"
categories: 教程
permalink: /cn/docs30/tutorial/kylin_client_tool.html
---

Apache Kylin Python 客户端工具库是基于Python可访问Kylin的客户端. 此工具库包含两个可使用组件. 

* Apache Kylin 命令行工具
* Apache Kylin SQLAchemy方言

想要了解更多关于此工具库信息请点击[Github仓库](https://github.com/Kyligence/kylinpy).

## 安装
请确保您python解释器版本在2.7+, 或者3.4+以上. 最方便安装Apache Kylin Python客户端工具库的方法是使用pip命令
```
    pip install --upgrade kylinpy
```

## Kylinpy 命令行工具
安装完kylinpy后, 立即可以在终端下访问kylinpy

```
    $ kylinpy
    Usage: kylinpy [OPTIONS] COMMAND [ARGS]...

    Options:
      -h, --host TEXT       Kylin host name  [required]
      -P, --port INTEGER    Kylin port, default: 7070
      -u, --username TEXT   Kylin username  [required]
      -p, --password TEXT   Kylin password  [required]
      --project TEXT        Kylin project  [required]
      --prefix TEXT         Kylin RESTful prefix of url, default: /kylin/api
      --debug / --no-debug  show debug infomation
      --api1 / --api2       API version; default is "api1"; "api1" 适用于 Apache Kylin
      --help                Show this message and exit.

    Commands:
      auth           get user auth info
      cube_columns   list cube columns
      cube_desc      show cube description
      cube_names     list cube names
      model_desc     show model description
      projects       list all projects
      query          sql query
      table_columns  list table columns
      table_names    list all table names
```

## Kylinpy命令行工具示例

1. 访问Apache Kylin
```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --api1 --debug auth
```

2. 访问选定cube所有的维度信息
```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --api1 --debug cube_columns --name kylin_sales_cube
```

3. 访问选定的cube描述
```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --api1 --debug cube_desc --name kylin_sales_cube
```

4. 访问所有cube名称
```
kylinpy -h hostname -u ADMIN -p KYLIN --project learn_kylin --api1 --debug cube_names
```

5. 访问选定cube的SQL定义
```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --api1 --debug cube_sql --name kylin_sales_cube
```

6. 列出Kylin中所有项目
```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --api1 --debug projects
```

7. 访问选定表所有的维度信息
```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --api1 --debug table_columns --name KYLIN_SALES
```

8. 访问所有表名
```
kylinpy -h hostname -u ADMIN -p KYLIN --project learn_kylin --api1 table_names
```

9. 访问所选模型信息
```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --api1 --debug model_desc --name kylin_sales_model
```

## Apache Kylin SQLAlchemy方言

任何一个使用SQLAlchemy的应用程序都可以通过此`方言`访问到Kylin, 您之前如果已经安装了kylinpy那么现在就已经集成好了SQLAlchemy Dialect. 请使用如下DSN模板访问Kylin

```
kylin://<username>:<password>@<hostname>:<port>/<project>?version=<v1|v2>&prefix=</kylin/api>
```

## SQLAlchemy 实例
测试Apache Kylin连接

```
    $ python
    >>> import sqlalchemy as sa
    >>> kylin_engine = sa.create_engine('kylin://username:password@hostname:7070/learn_kylin?version=v1')
    >>> results = kylin_engine.execute('SELECT count(*) FROM KYLIN_SALES')
    >>> [e for e in results]
    [(4953,)]
    >>> kylin_engine.table_names()
    [u'KYLIN_ACCOUNT',
     u'KYLIN_CAL_DT',
     u'KYLIN_CATEGORY_GROUPINGS',
     u'KYLIN_COUNTRY',
     u'KYLIN_SALES',
     u'KYLIN_STREAMING_TABLE']
```

### 使用 Python 和 Apache Kylin 做数据科学分析 

请参考此博客：[Use Python for Data Science with Apache Kylin](/blog/2019/06/26/use-python-for-data-science-with-apache-kylin/).