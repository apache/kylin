--
-- Copyright (C) 2020 Kyligence Inc. All rights reserved.
--
-- http://kyligence.io
--
-- This software is the confidential and proprietary information of
-- Kyligence Inc. ("Confidential Information"). You shall not disclose
-- such Confidential Information and shall use it only in accordance
-- with the terms of the license agreement you entered into with
-- Kyligence Inc.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
-- A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
-- OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
-- SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
-- LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
-- THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
-- (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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