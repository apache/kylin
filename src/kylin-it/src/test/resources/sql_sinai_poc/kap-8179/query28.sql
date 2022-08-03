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
SELECT "Z_360_OPPT_FINDER"."GAP_NAME" AS "GAP_NAME",
"Z_360_OPPT_FINDER"."MEMBER_LOB" AS "MEMBER_LOB",
"Z_360_OPPT_FINDER"."PAYER" AS "PAYER",
MIN(1) AS "TEMP_attr_Calculation_788692941300133889_qk__291183819__0_",
MIN(1) AS "TEMP_attr_Calculation_788692941300133889_qk__628667868__0_",
MIN(1) AS "TEMP_attr_Calculation_788692941300326402_qk__1391702898__0_",
MIN(1) AS "TEMP_attr_Calculation_788692941300326402_qk__4213059097__0_",
AVG("Z_360_OPPT_FINDER"."BEST_TARGET_PERCENT") AS "avg_BEST_TARGET_PERCENT_ok",
AVG((1 - (CASE WHEN (CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'No') THEN 1 ELSE NULL END) = 0
THEN NULL ELSE {fn CONVERT((CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" <> 'Yes')
THEN 1 ELSE 0 END), SQL_DOUBLE)} / (CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'No')
THEN 1 ELSE NULL END) END))) AS "avg_Calculation_432627069565870082_ok",
AVG(("Z_360_OPPT_FINDER"."BEST_TARGET_PERCENT" - (1 - (CASE WHEN (CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'No')
THEN 1 ELSE NULL END) = 0
THEN NULL ELSE {fn CONVERT((CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" <> 'Yes')
THEN 1 ELSE 0 END), SQL_DOUBLE)} / (CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'No')
THEN 1 ELSE NULL END) END)))) AS "avg_Calculation_788692941297889280_ok",
AVG(("Z_360_OPPT_FINDER"."GOOD_TARGET_PERCENT" - (1 - (CASE WHEN (CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'No') THEN 1 ELSE NULL END) = 0
THEN NULL ELSE {fn CONVERT((CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" <> 'Yes')
THEN 1 ELSE 0 END), SQL_DOUBLE)} / (CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'No')
THEN 1 ELSE NULL END) END)))) AS "avg_Color_Current_Great_Difference__copy__ok",
SUM({fn CONVERT((CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'No')
THEN 1 ELSE NULL END), SQL_BIGINT)}) AS "sum_Calculation_432627069565485057_ok",
SUM({fn CONVERT((CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 ELSE 0 END), SQL_BIGINT)}) AS "sum_Open_Gaps__copy__ok",
SUM((((CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'No')
THEN 1 ELSE NULL END) * "Z_360_OPPT_FINDER"."BEST_TARGET_PERCENT") - (CASE WHEN ("Z_360_OPPT_FINDER"."GAP_CLOSED" = 'Yes')
THEN 1 ELSE 0 END))) AS "sum___Gaps_to__Good__Target__copy__ok"
FROM "POPHEALTH_ANALYTICS"."Z_360_OPPT_FINDER" "Z_360_OPPT_FINDER"
WHERE (("Z_360_OPPT_FINDER"."PCP_NAME" = 'JONATHAN AREND')
AND ("Z_360_OPPT_FINDER"."GAP_CLOSED" IN ('No', 'Yes'))
AND ("Z_360_OPPT_FINDER"."LOOKBACK_TABLE" = 'CURRENT')
AND ("Z_360_OPPT_FINDER"."PAYER"
IN ('ACO', 'EMBLEM', 'EMPIRE', 'FIDELIS', 'HEALTHFIRST', 'UNITED')))
GROUP BY "Z_360_OPPT_FINDER"."GAP_NAME", "Z_360_OPPT_FINDER"."MEMBER_LOB", "Z_360_OPPT_FINDER"."PAYER";