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
select is_screen_on, count(1) as num from
(
select trans_id,
  case when TEST_ACCOUNT.ACCOUNT_ID >= 10000336 then 1
    else 2
    end as is_screen_on
from TEST_KYLIN_FACT
inner JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID
inner JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
 inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id
 inner JOIN edw.test_seller_type_dim as test_seller_type_dim
 ON test_kylin_fact.slr_segment_cd = test_seller_type_dim.seller_type_cd
 INNER JOIN TEST_ORDER as TEST_ORDER
 ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID
 inner JOIN test_country
ON test_account.account_country = test_country.country
)
group by is_screen_on