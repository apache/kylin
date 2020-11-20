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

SELECT "KYLIN_SALES"."PART_DT", SUM("KYLIN_SALES"."PRICE") AS "sum_PRICE_ok"
	FROM "KYLIN_SALES"
    INNER JOIN "KYLIN_CAL_DT" AS "KYLIN_CAL_DT" ON ("KYLIN_SALES"."PART_DT" = "KYLIN_CAL_DT"."CAL_DT")
    INNER JOIN (
     SELECT COUNT(1) AS "XTableau_join_flag",
      SUM("KYLIN_SALES"."PRICE") AS "X__alias__A",
       "KYLIN_SALES"."PART_DT" AS "none_CAL_DT_ok"   FROM "KYLIN_SALES"
         INNER JOIN "KYLIN_CAL_DT" AS "KYLIN_CAL_DT" ON ("KYLIN_SALES"."PART_DT" = "KYLIN_CAL_DT"."CAL_DT")
     GROUP BY "KYLIN_SALES"."PART_DT"   ORDER BY 2 DESC   LIMIT 10  ) "t0" ON ("KYLIN_SALES"."PART_DT" = "t0"."none_CAL_DT_ok")
    GROUP BY "KYLIN_SALES"."PART_DT"