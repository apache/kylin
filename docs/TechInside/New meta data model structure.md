#New meta data model structure
Kylin is doing a round code refactor which will introduce the following two changes on meta data:

* Abstract a "model_desc" layer from "cube_desc"

Before define a cube, user will need firstly define a model ("model_desc"); The model defines which is the fact table, which are lookup tables and how they be joined;

With the model be defined, when user define a cube ("cube_desc"), he/she only need to specify the table/column name for a dimension, as the join conditions have already been defined;

This abstraction is to extend the meta data to fulfill non-cube queries (coming soon);

* Support data tables from multiple hive databases;

User has the case that tables are from multiple hive database, and the table name might be the same; To support this case Kylin will use the database name + table name as the unique name for tables; And user need to specify the database name (if it is not "default") in SQL when query Kylin. 

Here is a sample; the fact table "test_kylin_fact" is from default hive database, you don't need to specify the db name; while lookup table is from "edw", you need use "edw.test_cal_dt" in the query:

	select test_cal_dt.Week_Beg_Dt, sum(price) as c1, count(1) as c2 from test_kylin_fact inner JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt  where test_kylin_fact.lstg_format_name='ABIN' 