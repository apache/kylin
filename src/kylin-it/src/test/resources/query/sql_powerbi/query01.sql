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


-- ISSUE #5527

-- failed at io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest.testPowerBiQuery()

SELECT
    "NAME"
,   COUNT("TRANS_ID")   AS  "C1"
,    CAST( SUM("TRANS_ID") AS DOUBLE) AS "C2"
FROM
    (
        SELECT
            "TRANS_ID"
        ,   "OTBL"."LSTG_FORMAT_NAME"
        ,   "OTBL"."CAL_DT"
        ,   "OTBL"."LEAF_CATEG_ID"
        ,   "OTBL"."LSTG_SITE_ID"
        ,   "OTBL"."ACCOUNT_COUNTRY"
        FROM
            (
                SELECT
                    "OTBL"."TRANS_ID"
                ,   "OTBL"."CAL_DT"
                ,   "OTBL"."LSTG_FORMAT_NAME"
                ,   "OTBL"."LEAF_CATEG_ID"
                ,   "OTBL"."LSTG_SITE_ID"
                ,   "OTBL"."PRICE"
                ,   "OTBL"."SELLER_ID"
                ,   "ITBL"."ACCOUNT_ID"
                ,   "ITBL"."ACCOUNT_BUYER_LEVEL"
                ,   "ITBL"."ACCOUNT_SELLER_LEVEL"
                ,   "ITBL"."ACCOUNT_COUNTRY"
                FROM
                    "DEFAULT"."TEST_KYLIN_FACT" AS  "OTBL"
                INNER JOIN
                "DEFAULT"."TEST_ACCOUNT"    AS  "ITBL"
                ON
                    "OTBL"."SELLER_ID"  =   "ITBL"."ACCOUNT_ID"
            ) AS OTBL
        INNER JOIN
            EDW.TEST_CAL_DT AS  TEST_CAL_DT
        ON
            OTBL.CAL_DT =   TEST_CAL_DT.CAL_DT
    ) AS OTBL
INNER JOIN
    TEST_CATEGORY_GROUPINGS
ON
    OTBL.LEAF_CATEG_ID  =   TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND OTBL.LSTG_SITE_ID   =   TEST_CATEGORY_GROUPINGS.SITE_ID
INNER JOIN
    EDW.TEST_SITES  AS  TEST_SITES
ON
    OTBL.LSTG_SITE_ID   =   TEST_SITES.SITE_ID
INNER JOIN
    "DEFAULT"."TEST_COUNTRY" AS  "ITBL"  ON  "OTBL"."ACCOUNT_COUNTRY"    =   "ITBL"."COUNTRY"
GROUP BY "NAME"
