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

SELECT EXTRACT(MONTH FROM "KYLIN_CAL_DT"."WEEK_BEG_DT") AS "mn_WEEK_BEG_DT_ok", (( EXTRACT(YEAR FROM "KYLIN_CAL_DT"."WEEK_BEG_DT") * 100) + EXTRACT(MONTH FROM "KYLIN_CAL_DT"."WEEK_BEG_DT")) AS "my_WEEK_BEG_DT_ok", QUARTER("KYLIN_CAL_DT"."WEEK_BEG_DT") AS "qr_WEEK_BEG_DT_ok", EXTRACT(YEAR FROM "KYLIN_CAL_DT"."WEEK_BEG_DT") AS "yr_WEEK_BEG_DT_ok"
FROM "KYLIN_SALES"
inner JOIN KYLIN_CAL_DT AS KYLIN_CAL_DT ON ("KYLIN_SALES"."PART_DT" = "KYLIN_CAL_DT"."CAL_DT")
 inner join kylin_category_groupings AS kylin_category_groupings
 on KYLIN_SALES.leaf_categ_id = kylin_category_groupings.leaf_categ_id and KYLIN_SALES.lstg_site_id = kylin_category_groupings.site_id
GROUP BY EXTRACT(YEAR FROM "KYLIN_CAL_DT"."WEEK_BEG_DT"), QUARTER("KYLIN_CAL_DT"."WEEK_BEG_DT"), (( EXTRACT(YEAR FROM "KYLIN_CAL_DT"."WEEK_BEG_DT") * 100) + EXTRACT(MONTH FROM "KYLIN_CAL_DT"."WEEK_BEG_DT")), EXTRACT(MONTH FROM "KYLIN_CAL_DT"."WEEK_BEG_DT")