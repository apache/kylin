# Kylin Release Test
Tags:3.x
## Prepare env
* Get kylin instance

* prepare data file from "release_test_0001.json"

* Create project "release_test_0001_project" and load table "load_table_list"


## MR engine

* Create model with "model_desc_data" in "release_test_0001_project"

* Create cube with "cube_desc_data" in "release_test_0001_project", cube name is "release_test_0001_cube"

* Build segment from "1325347200000" to "1356969600000" in "release_test_0001_cube"

* Build segment from "1356969600000" to "1391011200000" in "release_test_0001_cube"

* Merge cube "release_test_0001_cube" segment from "1325347200000" to "1391011200000"


## SPARK engine

* Clone cube "release_test_0001_cube" and name it "kylin_spark_cube" in "release_test_0001_project", modify build engine to "SPARK"

* Build segment from "1325347200000" to "1356969600000" in "kylin_spark_cube"

* Build segment from "1356969600000" to "1391011200000" in "kylin_spark_cube"

* Merge cube "kylin_spark_cube" segment from "1325347200000" to "1391011200000"


## FLINK engine

* Clone cube "release_test_0001_cube" and name it "kylin_flink_cube" in "release_test_0001_project", modify build engine to "FLINK"

* Build segment from "1325347200000" to "1356969600000" in "kylin_flink_cube"

* Build segment from "1356969600000" to "1391011200000" in "kylin_flink_cube"

* Merge cube "kylin_flink_cube" segment from "1325347200000" to "1391011200000"


## Query cube and pushdown

* Query SQL "select count(*) from kylin_sales" and specify "release_test_0001_cube" cube to query in "release_test_0001_project", compare result with "10000"

* Query SQL "select count(*) from kylin_sales" and specify "kylin_spark_cube" cube to query in "release_test_0001_project", compare result with "10000"

* Query SQL "select count(*) from kylin_sales" and specify "kylin_flink_cube" cube to query in "release_test_0001_project", compare result with "10000"

* Disable cube "release_test_0001_cube"

* Disable cube "kylin_spark_cube"

* Disable cube "kylin_flink_cube"

* Query SQL "select count(*) from kylin_sales" in "release_test_0001_project" and pushdown, compare result with "10000"



