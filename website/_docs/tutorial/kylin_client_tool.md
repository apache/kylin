---
layout: docs
title:  Kylin Client Tool Tutorial
categories: tutorial
permalink: /docs/tutorial/kylin_client_tool.html
version: v1.0
since: v1.2.0
---
  
> Kylin-client-tool is a tool coded with python, completely based on Kylin's restful apis.
> With it, users can easily create Kylin Cubes, timely build cubes, summit/schedule/check/cancel/resume jobs.

## Install
1.Make sure python2.6/2.7 installed

2.Two python packages `apscheduler` and `requests` are needed, run `setup.sh` to install. For mac users, please run`setup-mac.sh`. Or you can install them with setuptools.

## Settings
Modify settings/settings.py to set,

`KYLIN_USER`  Kylin's username

`KYLIN_PASSWORD`  Kylin's password

`KYLIN_REST_HOST`  Kylin's address

`KYLIN_REST_PORT`  Kylin's port

`KYLIN_JOB_MAX_COCURRENT`  Max concurrent jobs for cube building

`KYLIN_JOB_MAX_RETRY`  Max failover time after job failed

## About command lines
This tool is used by command lines, please run `python kylin_client_tool.py －h` to check for details.

## Cube's creation
This tool has its own cube definition method, the format is as below,

`cube name|fact table name|dimension1,type of dimension1;dimension2,type of dimension2...|measure1,expression of measure1,type of measure1...|settings|filter|`

`settings` has options below,

`no_dictionary`  Set dimensions not generate dictionaries in "Rowkeys" and their lengths

`mandatory_dimension`  Set mandatory dimensions in "Rowkeys"

`aggregation_group`  Set aggregation group

`partition_date_column`  Set partition date column

`partition_date_start`  Set partition start date

Cases can be found in file cube_def.csv, it does not support cube creation with lookup table

Use `-c` to create, use `-F` to specify cube definition file, for instance

`python kylin_client_tool.py -c -F cube_def.csv`

## Build cube
###Build cube with cube definition file
Use `-b` to build, use `-F` to specify the cube definition file, if "partition date column" specified, use `-T` to specify the end date(year-month-day), if not specified, it will take current time as end date, for instance,

`python kylin_client_tool.py -b -F cube_def.csv -T 2016-03-01`

###Build cube with file of cube names
Use `-f` to specify the file of cube names, each line of the file has one cube name, for instance,

`python kylin_client_tool.py -b -f cube_names.csv -T 2016-03-01`

###Build cube with cube names
Use `-C` to specify cube names, split with commas, for instance,

`python kylin_client_tool.py -b -C client_tool_test1,client_tool_test2 -T 2016-03-01`

## Job management
###Check jobs' statuses
Use `-s` to check, use `-f` to specify file of cube names, use `-C` to specify cube names, if not specified, it will check all cubes' statuses. Use `-S` to specify jobs' statuses, R for `Running`, E for `Error`, F for `Finished`, D for `Discarded`, for instance,

`python kylin_client_tool.py -s -C kylin_sales_cube -f cube_names.csv -S F`

###Resume jobs
Use `-r` to resume jobs, use `-f` to specify file of cube names, use `-C` to specify cube names, if not specified, it will reuse all jobs with error status, for instance,

`python kylin_client_tool.py -r -C kylin_sales_cube -f cube_names.csv`

###Cancel jobs
Use `-k` to cancel jobs, use `-f` to specify file with cube names, use `-C` to specify cube names, if not specified, it will cancel all jobs with running or error statuses, for instance,

`python kylin_client_tool.py -k -C kylin_sales_cube -f cube_names.csv`

## Schedule cubes building
### Build cube at set intervals
On the basis of command of cube building, use `-B i` to specify the mode of build cube at set intervals, use `-O` to specify how many hours, for instance,

`python kylin_client_tool.py -b -F cube_def.csv -B i -O 1`

### Build cube at given time
Use `-B t` to specify the mode of build cube at given time, use `-O` to specify the build time, split with commas,for instance,

`python kylin_client_tool.py -b -F cube_def.csv -T 2016-03-04 -B t -O 2016,3,1,0,0,0`
