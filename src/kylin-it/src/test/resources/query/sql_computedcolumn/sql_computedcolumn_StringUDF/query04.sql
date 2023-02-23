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


SELECT KYLIN_SALES.PRICE,
KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_NAME,
KYLIN_SALES.LSTG_FORMAT_NAME,
count(1),
count(DISTINCT KYLIN_SALES.PRICE + 1),
count(DISTINCT substring(KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_NAME, 0, 4)),
count(DISTINCT CONCAT(KYLIN_SALES.LSTG_FORMAT_NAME, KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_NAME))
FROM TEST_KYLIN_FACT as KYLIN_SALES
JOIN TEST_CATEGORY_GROUPINGS KYLIN_CATEGORY_GROUPINGS ON KYLIN_SALES.LEAF_CATEG_ID = KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID
GROUP BY KYLIN_SALES.PRICE,
KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_NAME,
KYLIN_SALES.LSTG_FORMAT_NAME
