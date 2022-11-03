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

SELECT  SUM("TEST_KYLIN_FACT"."PRICE") AS "sum_PRICE_ok" 
	FROM "TEST_KYLIN_FACT"
    INNER JOIN "EDW"."TEST_CAL_DT" AS "TEST_CAL_DT" ON ("TEST_KYLIN_FACT"."CAL_DT" = "TEST_CAL_DT"."CAL_DT")
     inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id

    INNER JOIN (
     SELECT COUNT(1) AS "XTableau_join_flag",
      SUM("TEST_KYLIN_FACT"."PRICE") AS "X__alias__A",
       "TEST_KYLIN_FACT"."CAL_DT" AS "none_CAL_DT_ok"   FROM "TEST_KYLIN_FACT"
         INNER JOIN "EDW"."TEST_CAL_DT" AS "TEST_CAL_DT" ON ("TEST_KYLIN_FACT"."CAL_DT" = "TEST_CAL_DT"."CAL_DT")
          inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id
     GROUP BY "TEST_KYLIN_FACT"."CAL_DT"   ORDER BY 2 DESC   LIMIT 10  ) "t0" ON ("TEST_KYLIN_FACT"."CAL_DT" = "t0"."none_CAL_DT_ok") 
    GROUP BY "TEST_KYLIN_FACT"."CAL_DT"
