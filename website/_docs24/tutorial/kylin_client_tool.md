---
layout: docs24
title:  Kylin Python Client
categories: tutorial
permalink: /docs24/tutorial/kylin_client_tool.html
---

Apache Kylin Python Client Library is a python-based Apache Kylin client. There are two components in Apache Kylin Python Client Library:

* Apache Kylin command line tools
* Apache Kylin dialect for SQLAlchemy

You can get more detail from this [Github Repository](https://github.com/Kyligence/kylinpy).

## Installation
Make sure Python version is 2.7+ or 3.4+. The easiest way to install Apache Kylin Python Client Library is to use "pip":

```
pip install --upgrade kylinpy
```

## Kylinpy CLI
After installing Apache Kylin Python Client Library you may run kylinpy in terminal.

```
$ kylinpy
Usage: kylinpy [OPTIONS] COMMAND [ARGS]...

Options:
  -h, --host TEXT       Kylin host name  [required]
  -P, --port INTEGER    Kylin port, default: 7070
  -u, --username TEXT   Kylin username  [required]
  -p, --password TEXT   Kylin password  [required]
  --project TEXT        Kylin project  [required]
  --prefix TEXT         Kylin RESTful prefix of url, default: "/kylin/api"
  --debug / --no-debug  show debug infomation
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

## Examples for Kylinpy CLI

1. To get all user info from Apache Kylin with debug mode

```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --debug auth
```

2. To get all cube columns from Apache Kylin with debug mode

```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --debug cube_columns --name kylin_sales_cube
```

3. To get cube description of selected cube from Apache Kylin with debug mode

```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --debug cube_desc --name kylin_sales_cube
```

4. To get all cube names from Apache Kylin with debug mode

```
kylinpy -h hostname -u ADMIN -p KYLIN --project learn_kylin --debug cube_names
```

5. To get cube SQL of selected cube from Apache Kylin with debug mode

```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --debug cube_sql --name kylin_sales_cube
```

6. To list all projects from Apache Kylin with debug mode

```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --debug projects
```

7. To list all tables column of selected cube from Apache Kylin with debug mode

```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --debug table_columns --name KYLIN_SALES
```

8. To get all table names from kylin

```
kylinpy -h hostname -u ADMIN -p KYLIN --project learn_kylin --api1 table_names
```

9. To get the model description of the selected model from Apache Kylin with debug mode

```
kylinpy -h hostname -P 7070 -u ADMIN -p KYLIN --project learn_kylin --debug model_desc --name kylin_sales_model
```

## Kylin dialect for SQLAlchemy

Any application that uses SQLAlchemy can now query Apache Kylin with this Apache Kylin dialect installed. It is part of the Apache Kylin Python Client Library, so if you already installed this library in the previous step, you are ready to use. You may use below template to build DSN to connect Apache Kylin.

```
kylin://<username>:<password>@<hostname>:<port>/<project>?version=<v1|v2>&prefix=</kylin/api>
```

## Examples for SQLAlchemy

Test connection with Apache Kylin

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


