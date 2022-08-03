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
SELECT (CASE WHEN ("Z_PROVDASH_UM_ED"."COPD" = 1)
THEN 'Yes' ELSE 'No ' END) AS "COPD__copy_",
(CASE WHEN ("Z_PROVDASH_UM_ED"."STROKE" = 1) 
THEN 'Yes' ELSE 'No ' END) AS "Calculation_540713452419612672",
(CASE WHEN ("Z_PROVDASH_UM_ED"."DIABETES" = 1) THEN 'Yes' ELSE 'No ' END) AS "Calculation_540713452420141057",
{fn CONCAT({fn CONCAT("Z_PROVDASH_UM_ED"."PAYER", ' ')}, "Z_PROVDASH_UM_ED"."LOB")} AS "Calculation_886083264414601216",   
(CASE WHEN ("Z_PROVDASH_UM_ED"."AMI" = 1) THEN 'Yes' ELSE 'No ' END) AS "Hx_of_CHF__copy_",
(CASE WHEN ("Z_PROVDASH_UM_ED"."VASCULAR_DISEASE" = 1) THEN 'Yes' ELSE 'No ' END) AS "Hx_of_Diabetes__copy_",
(CASE WHEN ("Z_PROVDASH_UM_ED"."CHF" = 1) THEN 'Yes' ELSE 'No ' END) AS "Hx_of_Stroke__copy_",
"Z_PROVDASH_UM_ED"."MARA_MEDICARE_RISK_SCORE" AS "MARA_MEDICARE_RISK_SCORE__copy_",   
"Z_PROVDASH_UM_ED"."MEMBER_ID" AS "MEMBER_ID",   
"Z_PROVDASH_UM_ED"."MEMBER_NAME" AS "MEMBER_NAME",   
"Z_PROVDASH_UM_ED"."PHONE" AS "PHONE",   
"Z_PROVDASH_UM_ED"."STATE" AS "STATE",   
"Z_PROVDASH_UM_ED"."ZIP" AS "ZIP",   
MIN("Z_PROVDASH_UM_ED"."MARA_MEDICARE_RISK_SCORE") AS "max_MARA_MEDICARE_RISK_SCORE__copy__ok",   
MIN("Z_PROVDASH_UM_ED"."MARA_MEDICARE_RISK_SCORE") AS "max_MARA_MEDICARE_RISK_SCORE_ok",   
((({fn YEAR("Z_PROVDASH_UM_ED"."DOB")} * 10000) + ({fn MONTH("Z_PROVDASH_UM_ED"."DOB")} * 100)) + {fn DAYOFMONTH("Z_PROVDASH_UM_ED"."DOB")}) AS "md_DOB_ok" 
FROM "POPHEALTH_ANALYTICS"."Z_PROVDASH_UM_ED" "Z_PROVDASH_UM_ED" 
WHERE ("Z_PROVDASH_UM_ED"."FULL_NAME" = 'JONATHAN AREND') 
GROUP BY (CASE WHEN ("Z_PROVDASH_UM_ED"."COPD" = 1) THEN 'Yes' ELSE 'No ' END),
(CASE WHEN ("Z_PROVDASH_UM_ED"."STROKE" = 1) THEN 'Yes' ELSE 'No ' END),
(CASE WHEN ("Z_PROVDASH_UM_ED"."DIABETES" = 1) THEN 'Yes' ELSE 'No ' END),
{fn CONCAT({fn CONCAT("Z_PROVDASH_UM_ED"."PAYER", ' ')}, "Z_PROVDASH_UM_ED"."LOB")},   
(CASE WHEN ("Z_PROVDASH_UM_ED"."AMI" = 1) THEN 'Yes' ELSE 'No ' END),
(CASE WHEN ("Z_PROVDASH_UM_ED"."VASCULAR_DISEASE" = 1) THEN 'Yes' ELSE 'No ' END),
(CASE WHEN ("Z_PROVDASH_UM_ED"."CHF" = 1) THEN 'Yes' ELSE 'No ' END),
"Z_PROVDASH_UM_ED"."MARA_MEDICARE_RISK_SCORE",  
 "Z_PROVDASH_UM_ED"."MEMBER_ID",   
 "Z_PROVDASH_UM_ED"."MEMBER_NAME",   
 "Z_PROVDASH_UM_ED"."PHONE",   
 "Z_PROVDASH_UM_ED"."STATE",   "Z_PROVDASH_UM_ED"."ZIP",   
 ((({fn YEAR("Z_PROVDASH_UM_ED"."DOB")} * 10000) + ({fn MONTH("Z_PROVDASH_UM_ED"."DOB")} * 100)) + {fn DAYOFMONTH("Z_PROVDASH_UM_ED"."DOB")});