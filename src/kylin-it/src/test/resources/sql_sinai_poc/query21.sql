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
SELECT "Z_360_HEAT"."GAP_NAME" AS "GAP_NAME",
"Z_360_HEAT"."HCC_NAME" AS "HCC_NAME",
"Z_360_HEAT"."MEMBER_ID" AS "MEMBER_ID",
"Z_360_HEAT"."MEMBER_NAME" AS "MEMBER_NAME",
"Z_360_HEAT"."MRN" AS "MRN",
"Z_360_HEAT"."PAYER_LOB" AS "PAYER_LOB",
AVG("Z_360_HEAT"."HCC_RAF_SCORE") AS "avg_HCC_RAF_SCORE_ok",
AVG("Z_360_HEAT"."INCREMENTAL_RAF") AS "avg_INCREMENTAL_RAF_ok",
AVG("Z_360_HEAT"."TOTAL_RAF_SCORE") AS "avg_TOTAL_RAF_SCORE_ok",
((({fn YEAR("Z_360_HEAT"."MEMBER_DOB")} * 10000)
+ ({fn MONTH("Z_360_HEAT"."MEMBER_DOB")} * 100))
+ {fn DAYOFMONTH("Z_360_HEAT"."MEMBER_DOB")}) AS "md_MEMBER_DOB_ok"
FROM "POPHEALTH_ANALYTICS"."Z_360_HEAT" "Z_360_HEAT"
WHERE (('Patients by HCC Opportunity' = 'Patients by HCC Opportunity')
AND ("Z_360_HEAT"."PCP_NAME" = 'JONATHAN AREND')
AND ("Z_360_HEAT"."PCP_NAME" = 'JONATHAN AREND')
AND ("Z_360_HEAT"."GAP_TYPE" = 'HCC SUSPECT'))
GROUP BY "Z_360_HEAT"."GAP_NAME",   "Z_360_HEAT"."HCC_NAME",   "Z_360_HEAT"."MEMBER_ID",   "Z_360_HEAT"."MEMBER_NAME",   "Z_360_HEAT"."MRN",   "Z_360_HEAT"."PAYER_LOB",   ((({fn YEAR("Z_360_HEAT"."MEMBER_DOB")} * 10000) + ({fn MONTH("Z_360_HEAT"."MEMBER_DOB")} * 100)) + {fn DAYOFMONTH("Z_360_HEAT"."MEMBER_DOB")});