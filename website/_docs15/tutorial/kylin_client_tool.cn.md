---
layout: docs15-cn
title:  Kylin Client Tool 使用教程
categories: 教程
permalink: /cn/docs15/tutorial/kylin_client_tool.html
---
  
> Kylin-client-tool是一个用python编写的，完全基于kylin的rest api的工具。可以实现kylin的cube创建，按时build cube，job的提交、调度、查看、取消与恢复。
  
## 安装
1.确认运行环境安装了python2.6/2.7

2.本工具需安装第三方python包apscheduler和requests，运行setup.sh进行安装，mac用户运行setup-mac.sh进行安装。也可用setuptools进行安装

## 配置
修改工具目录下的settings/settings.py文件，进行配置

`KYLIN_USER`  Kylin用户名

`KYLIN_PASSWORD`  Kylin的密码

`KYLIN_REST_HOST`  Kylin的地址

`KYLIN_REST_PORT`  Kylin的端口

`KYLIN_JOB_MAX_COCURRENT`  允许同时build的job数量

`KYLIN_JOB_MAX_RETRY`  cube build出现error后，允许的重启job次数

## 命令行的使用
本工具使用optparse通过命令行来执行操作，具体用法可通过`python kylin_client_tool.py －h`来查看

## cube的创建
本工具定义了一种读手写的文本，来快速cube创建的方法，格式如下

`cube名|fact table名|维度1,维度1类型;维度2,维度2类型...|指标1,指标1表达式,指标1类型...|设置项|filter|`

设置项内有以下选项，

`no_dictionary`  设置Rowkeys中不生成dictionary的维度及其长度

`mandatory_dimension`  设置Rowkeys中mandatory的维度

`aggregation_group`  设置aggregation group

`partition_date_column`  设置partition date column

`partition_date_start`  设置partition start date

具体例子可以查看cube_def.csv文件，目前不支持含lookup table的cube创建

使用`-c`命令进行创建，用`-F`指定cube定义文件，例如

`python kylin_client_tool.py -c -F cube_def.csv`

## build cube
###使用cube定义文件build
使用`-b`命令，需要用`-F`指定cube定义文件，如果指定了partition date column，通过`-T`指定end date(year-month-day格式)，若不指定，以当前时间为end date，例如

`python kylin_client_tool.py -b -F cube_def.csv -T 2016-03-01`

###使用cube名文件build
用`-f`指定cube名文件，文件每行一个cube名

`python kylin_client_tool.py -b -f cube_names.csv -T 2016-03-01`

###直接命令行写cube名build
用`-C`指定cube名，通过逗号进行分隔

`python kylin_client_tool.py -b -C client_tool_test1,client_tool_test2 -T 2016-03-01`

## job管理
###查看job状态
使用`-s`命令查看，用`-f`指定cube名文件，用`-C`指定cube名，若不指定，将查看所有cube状态。用`-S`指定job状态，R表示`Running`，E表示`Error`，F表示`Finished`，D表示`Discarded`，例如：

`python kylin_client_tool.py -s -C kylin_sales_cube -f cube_names.csv -S F`

###恢复job
用`-r`命令恢复job，用`-f`指定cube名文件，用`-C`指定cube名，若不指定，将恢复所有Error状态的job，例如：

`python kylin_client_tool.py -r -C kylin_sales_cube -f cube_names.csv`

###取消job
用`-k`命令取消job，用`-f`指定cube名文件，用`-C`指定cube名，若不指定，将取消所有Running或Error状态的job，例如：

`python kylin_client_tool.py -k -C kylin_sales_cube -f cube_names.csv`

## 定时build cube
### 每隔一段时间build cube
在cube build命令的基础上，使用`-B i`指定每隔一段时间build的方式，使用`-O`指定间隔的小时数，例如：

`python kylin_client_tool.py -b -F cube_def.csv -B i -O 1`

### 设定时间build cube
使用`-B t`指定按时build cube的方式，使用`-O`指定build时间，用逗号进行分隔

`python kylin_client_tool.py -b -F cube_def.csv -T 2016-03-04 -B t -O 2016,3,1,0,0,0`
