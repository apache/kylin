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

SELECT
  "TEST_CATEGORY_GROUPINGS"."CATEG_LVL2_NAME" AS "CATEG_LVLC_NAME",
  "TEST_CATEGORY_GROUPINGS"."CATEG_LVL3_NAME" AS "CATEG_LVLD_NAME",
  "TEST_CATEGORY_GROUPINGS"."META_CATEG_NAME" AS "META_CATEG_NAME",
  1 AS "Number_of_Records",
  "TEST_CATEGORY_GROUPINGS"."UPD_DATE" AS "UPD_DATE",
  "TEST_CATEGORY_GROUPINGS"."UPD_USER" AS "UPD_USER",
  "TEST_CATEGORY_GROUPINGS"."USER_DEFINED_FIELD1" AS "USER_DEFINED_FIELDB",
  "TEST_CATEGORY_GROUPINGS"."USER_DEFINED_FIELD3" AS "USER_DEFINED_FIELDD"
FROM "TEST_CATEGORY_GROUPINGS" "TEST_CATEGORY_GROUPINGS"
LIMIT 10000
;{"scanRowCount":144,"scanBytes":0,"scanFiles":1,"cuboidId":[-1]}
