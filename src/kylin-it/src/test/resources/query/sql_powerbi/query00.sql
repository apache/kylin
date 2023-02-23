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

select count(*) as CNT, "LSTG_FORMAT_NAME" from "DEFAULT"."TEST_KYLIN_FACT" as "OTBL" inner join "DEFAULT"."TEST_ACCOUNT" as "ITBL" on (("OTBL"."SELLER_ID" = "ITBL"."ACCOUNT_ID" and "OTBL"."SELLER_ID" is not null) and "ITBL"."ACCOUNT_ID" is not null or "OTBL"."SELLER_ID" is null and "ITBL"."ACCOUNT_ID" is null)
inner join "EDW"."TEST_CAL_DT" as "DTBL" on (("OTBL"."CAL_DT" = "DTBL"."CAL_DT" and "OTBL"."CAL_DT" is not null) and "DTBL"."CAL_DT" is not null or "OTBL"."CAL_DT" is null and "DTBL"."CAL_DT" is null)
inner join "TEST_CATEGORY_GROUPINGS" AS "TEST_CATEGORY_GROUPINGS"
on "OTBL"."LEAF_CATEG_ID" = "TEST_CATEGORY_GROUPINGS"."LEAF_CATEG_ID" and "OTBL"."LSTG_SITE_ID" = "TEST_CATEGORY_GROUPINGS"."SITE_ID"
group by "LSTG_FORMAT_NAME"
