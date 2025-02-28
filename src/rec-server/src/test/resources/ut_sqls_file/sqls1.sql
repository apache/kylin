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
-- grammar invalid sqls
SELECT {fn DAYOFMONTH({fn CURRENT_DATE() }) } Calculation_330451678684426242 FROM "DEFAULT".KYLIN_ACCOUNT KYLIN_ACCOUNT LIMIT 500;
SELECT PART_DT, SUM(PRICE) FROM (SELECT * FROM KYLIN_SALES WHERE PART_DT = DATE '2010-01-01') CB_VIEW WHERE LSTG_FORMAT_NAME IN ('A') AND OPS_USER_ID IN ('A') AND OPS_REGION IN ('A') GROUP BY PART_DT LIMIT 500;
SELECT PART_DT, SUM(PRICE) FROM (SELECT * FROM KYLIN_SALES WHERE PART_DT = DATE '2010-01-01') CB_VIEW WHERE LSTG_FORMAT_NAME IN ('A') AND OPS_USER_ID IN ('A') AND OPS_REGION IN ('A') AND LSTG_FORMAT_NAME IN ('A') AND OPS_REGION IN ('A') AND OPS_USER_ID IN ('A') GROUP BY PART_DT LIMIT 500;
SELECT SELLER_ID FROM KYLIN_SALES WHERE SELLER_ID NOT IN (1) GROUP BY SELLER_ID HAVING SELLER_ID NOT IN (1) LIMIT 500;
SELECT TRANS_ID, SUM(PRICE) FROM KYLIN_SALES WHERE PART_DT = '2010-01-01' GROUP BY TRANS_ID LIMIT 500;
SELECT COUNT(DISTINCT KYLIN_SALES.PART_DT) FROM KYLIN_SALES KYLIN_SALES INNER JOIN KYLIN_CAL_DT KYLIN_CAL_DT ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.CAL_DT INNER JOIN KYLIN_CATEGORY_GROUPINGS KYLIN_CATEGORY_GROUPINGS ON KYLIN_SALES.LEAF_CATEG_ID = KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND KYLIN_SALES.LSTG_SITE_ID = KYLIN_CATEGORY_GROUPINGS.SITE_ID LIMIT 500;

-- grammar valid sqls
SELECT CAL_DT, LSTG_FORMAT_NAME, SUM(PRICE) FROM TEST_KYLIN_FACT WHERE CAL_DT = '2012-01-02' GROUP BY CAL_DT, LSTG_FORMAT_NAME;
SELECT CAL_DT, SUM(PRICE) FROM TEST_KYLIN_FACT WHERE CAL_DT = '2012-01-02' GROUP BY CAL_DT;