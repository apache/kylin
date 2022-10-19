--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--


SELECT "CAL_DT" ,
       SUM ("TEMP_Calculation_54915774428294") AS "TEMP_Calculation_54915774428294_1",
               COUNT (DISTINCT "CAL_DT") AS "TEMP_Calculation_97108873613918",
                     COUNT (DISTINCT (CASE
                                          WHEN ("x_measure__0" > 0) THEN "LSTG_FORMAT_NAME"
                                          ELSE CAST (NULL AS VARCHAR (1))
                                      END)) AS "TEMP_Calculation_97108873613911"
FROM

( select "自定义 SQL 查询"."CAL_DT" , SUM ("自定义 SQL 查询"."SELLER_ID") AS "TEMP_Calculation_54915774428294",
         "t0"."x_measure__0", "t0"."LSTG_FORMAT_NAME"
  from
          (SELECT *
           FROM TEST_KYLIN_FACT) "自定义 SQL 查询"
        INNER JOIN
             (SELECT LSTG_FORMAT_NAME, ORDER_ID, SUM ("PRICE") AS "X_measure__0"
              FROM TEST_KYLIN_FACT  GROUP  BY LSTG_FORMAT_NAME, ORDER_ID) "t0" ON "自定义 SQL 查询"."ORDER_ID" = "t0"."ORDER_ID"
  group by "自定义 SQL 查询"."CAL_DT","t0"."x_measure__0", "t0"."LSTG_FORMAT_NAME"
)

GROUP  BY "CAL_DT"
