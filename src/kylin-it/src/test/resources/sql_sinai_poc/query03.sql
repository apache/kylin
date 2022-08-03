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
SELECT "t2"."FULL_NAME" AS "FULL_NAME",
"t5"."X_measure__1" AS "avg_Calculation_578712577211949057_ok",
"t2"."X_measure__3" AS "avg_MARA_PROSPECTIVE_RISK_PRACTICE__copy__ok",
{fn CONVERT("t6"."X_measure__4", SQL_DOUBLE)} AS "avg_MARA_PROSPECTIVE_RISK_Provider__copy__ok"
FROM (   SELECT "t0"."FULL_NAME" AS "FULL_NAME",
AVG("t1"."X_measure__2") AS "X_measure__3"
FROM ( SELECT "Z_PROVDASH_UM_ED"."DEFAULT_PRACTICE" AS "DEFAULT_PRACTICE",
"Z_PROVDASH_UM_ED"."FULL_NAME" AS "FULL_NAME"
FROM "POPHEALTH_ANALYTICS"."Z_PROVDASH_UM_ED" "Z_PROVDASH_UM_ED"
WHERE ("Z_PROVDASH_UM_ED"."FULL_NAME" = 'JONATHAN AREND')
GROUP BY "Z_PROVDASH_UM_ED"."DEFAULT_PRACTICE", "Z_PROVDASH_UM_ED"."FULL_NAME") "t0"
INNER JOIN ( SELECT "Z_PROVDASH_UM_ED"."DEFAULT_PRACTICE" AS "DEFAULT_PRACTICE",
AVG("Z_PROVDASH_UM_ED"."MARA_PROSPECTIVE_RISK") AS "X_measure__2"
FROM "POPHEALTH_ANALYTICS"."Z_PROVDASH_UM_ED" "Z_PROVDASH_UM_ED"     
GROUP BY "Z_PROVDASH_UM_ED"."DEFAULT_PRACTICE"   ) "t1" 
ON (("t0"."DEFAULT_PRACTICE" = "t1"."DEFAULT_PRACTICE") 
OR (("t0"."DEFAULT_PRACTICE" IS NULL) 
AND ("t1"."DEFAULT_PRACTICE" IS NULL)))   
GROUP BY "t0"."FULL_NAME" ) "t2"   
INNER JOIN (   SELECT "t3"."FULL_NAME" AS "FULL_NAME",     
{fn CONVERT("t4"."X_measure__0", SQL_DOUBLE)} AS "X_measure__1"   
FROM (     SELECT 'Source UM' AS "Calculation_59954192150036482", "Z_PROVDASH_UM_ED"."FULL_NAME" AS "FULL_NAME"     
FROM "POPHEALTH_ANALYTICS"."Z_PROVDASH_UM_ED" "Z_PROVDASH_UM_ED"     
WHERE ("Z_PROVDASH_UM_ED"."FULL_NAME" = 'JONATHAN AREND')     
GROUP BY "Z_PROVDASH_UM_ED"."FULL_NAME") "t3"     
INNER JOIN (  SELECT 'Source UM' AS "Calculation_59954192150036482", 
AVG("Z_PROVDASH_UM_ED"."MARA_PROSPECTIVE_RISK") AS "X_measure__0"     
FROM "POPHEALTH_ANALYTICS"."Z_PROVDASH_UM_ED" "Z_PROVDASH_UM_ED"     
HAVING (COUNT(1) > 0)) "t4" ON ("t3"."Calculation_59954192150036482" = "t4"."Calculation_59954192150036482") ) "t5" 
ON (("t2"."FULL_NAME" = "t5"."FULL_NAME") OR (("t2"."FULL_NAME" IS NULL) 
AND ("t5"."FULL_NAME" IS NULL)))   
INNER JOIN ( SELECT "Z_PROVDASH_UM_ED"."FULL_NAME" AS "FULL_NAME", 
AVG("Z_PROVDASH_UM_ED"."MARA_PROSPECTIVE_RISK") AS "X_measure__4"   
FROM "POPHEALTH_ANALYTICS"."Z_PROVDASH_UM_ED" "Z_PROVDASH_UM_ED"   
GROUP BY "Z_PROVDASH_UM_ED"."FULL_NAME" ) "t6" 
ON (("t2"."FULL_NAME" = "t6"."FULL_NAME") 
OR (("t2"."FULL_NAME" IS NULL) 
AND ("t6"."FULL_NAME" IS NULL)));