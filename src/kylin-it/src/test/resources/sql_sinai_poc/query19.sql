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
SELECT "t2"."X_measure__1" AS "avg_LOD_Network_Recapture__copy__ok",
"t1"."X_measure__3" AS "avg_LOD_Practice_HCC_RAF_Score__copy_2__ok",
"t4"."X_measure__5" AS "avg_LOD_Provider_RAF_Score__copy_2__ok"
FROM (   SELECT AVG("t0"."X_measure__2") AS "X_measure__3"
FROM (     SELECT "Z_360_HEAT"."PCP_PRACTICE" AS "PCP_PRACTICE",
AVG("Z_360_HEAT"."TOTAL_RAF_SCORE") AS "X_measure__2"
FROM "POPHEALTH_ANALYTICS"."Z_360_HEAT" "Z_360_HEAT"
GROUP BY "Z_360_HEAT"."PCP_PRACTICE") "t0"
HAVING (COUNT(1) > 0) ) "t1"
CROSS JOIN (   SELECT {fn CONVERT(AVG("Z_360_HEAT"."TOTAL_RAF_SCORE"), SQL_DOUBLE)} AS "X_measure__1"
FROM "POPHEALTH_ANALYTICS"."Z_360_HEAT" "Z_360_HEAT"
HAVING (COUNT(1) > 0) ) "t2"
CROSS JOIN (   SELECT AVG("t3"."X_measure__4") AS "X_measure__5"
FROM (     SELECT "Z_360_HEAT"."PCP_NAME" AS "PCP_NAME",
AVG("Z_360_HEAT"."TOTAL_RAF_SCORE") AS "X_measure__4"
FROM "POPHEALTH_ANALYTICS"."Z_360_HEAT" "Z_360_HEAT"
GROUP BY "Z_360_HEAT"."PCP_NAME") "t3"
HAVING (COUNT(1) > 0) ) "t4";