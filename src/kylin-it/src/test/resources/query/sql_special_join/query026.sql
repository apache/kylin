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

-- ISSUE #3106

SELECT
  "NAME",
  count(*) AS "C1"
FROM (SELECT
  "OTBL"."TRANS_ID",
  "ITBL"."NAME"
FROM (SELECT
  "OTBL"."TRANS_ID",
  "OTBL"."CAL_DT",
  "OTBL"."LSTG_FORMAT_NAME",
  "OTBL"."LEAF_CATEG_ID",
  "OTBL"."LSTG_SITE_ID",
  "OTBL"."PRICE",
  "OTBL"."SELLER_ID",
  "ITBL"."ACCOUNT_ID",
  "ITBL"."ACCOUNT_BUYER_LEVEL",
  "ITBL"."ACCOUNT_SELLER_LEVEL",
  "ITBL"."ACCOUNT_COUNTRY"
FROM "DEFAULT"."TEST_KYLIN_FACT" AS "OTBL"
INNER JOIN "DEFAULT"."TEST_ACCOUNT" AS "ITBL"
  ON ("OTBL"."SELLER_ID" = "ITBL"."ACCOUNT_ID")
  INNER JOIN edw.test_cal_dt as test_cal_dt
ON OTBL.cal_dt = test_cal_dt.cal_dt
inner JOIN test_category_groupings
 ON OTBL.leaf_categ_id = test_category_groupings.leaf_categ_id AND OTBL.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON OTBL.lstg_site_id = test_sites.site_id) AS "OTBL"
INNER JOIN "DEFAULT"."TEST_COUNTRY" AS "ITBL"
  ON ("OTBL"."ACCOUNT_COUNTRY" = "ITBL"."COUNTRY")) AS "ITBL"

GROUP BY "NAME"