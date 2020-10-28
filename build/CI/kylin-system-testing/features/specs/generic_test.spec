Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

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


SPARK engine

Clone cube "release_test_0001_cube" and name it "kylin_spark_cube" in "release_test_0001_project", modify build engine to "SPARK"

Build segment from "1325347200000" to "1356969600000" in "kylin_spark_cube"

Build segment from "1356969600000" to "1391011200000" in "kylin_spark_cube"

Merge cube "kylin_spark_cube" segment from "1325347200000" to "1391011200000"


## Query cube and pushdown

* Query SQL "select count(*) from kylin_sales" and specify "release_test_0001_cube" cube to query in "release_test_0001_project", compare result with "10000"

Query SQL "select count(*) from kylin_sales" and specify "kylin_spark_cube" cube to query in "release_test_0001_project", compare result with "10000"

* Disable cube "release_test_0001_cube"

Disable cube "kylin_spark_cube"

* Query SQL "select count(*) from kylin_sales" in "release_test_0001_project" and pushdown, compare result with "10000"



