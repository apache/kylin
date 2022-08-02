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

-- block by https://github.com/Kyligence/KAP/issues/6548, work on calcite

select info.lstg_format_name, GMV, TRANS_CNT from
(
select TEST_KYLIN_FACT.lstg_format_name, count(*) as TRANS_CNT  from
TEST_KYLIN_FACT

inner JOIN
(select TEST_KYLIN_FACT.cal_dt,week_beg_dt, ITEM_COUNT, LSTG_SITE_ID, SELLER_ID from TEST_KYLIN_FACT inner join EDW.TEST_CAL_DT on TEST_KYLIN_FACT.CAL_DT =  TEST_CAL_DT.cal_dt where week_beg_dt >= DATE '2012-04-10') xxx
ON TEST_KYLIN_FACT.CAL_DT = xxx.cal_dt

inner JOIN TEST_CATEGORY_GROUPINGS ON TEST_KYLIN_FACT.leaf_categ_id = TEST_CATEGORY_GROUPINGS.leaf_categ_id AND TEST_KYLIN_FACT.lstg_site_id = TEST_CATEGORY_GROUPINGS.site_id

inner JOIN
(select cal_dt,week_beg_dt from EDW.TEST_CAL_DT where week_beg_dt >= DATE '2013-01-01' order by week_beg_dt ) xxx2 ON TEST_KYLIN_FACT.CAL_DT = xxx2.cal_dt

inner JOIN TEST_CATEGORY_GROUPINGS as test_sites
ON TEST_KYLIN_FACT.lstg_site_id = test_sites.site_id

where TEST_CATEGORY_GROUPINGS.meta_categ_name <> 'Baby' and test_sites.site_id <> 1
group by TEST_KYLIN_FACT.lstg_format_name
) as info1
inner join
(
    select TEST_KYLIN_FACT.lstg_format_name, sum(PRICE) as GMV, COUNT(*) as CNT from TEST_KYLIN_FACT group by TEST_KYLIN_FACT.lstg_format_name
) as info
on info1.lstg_format_name = info.lstg_format_name
