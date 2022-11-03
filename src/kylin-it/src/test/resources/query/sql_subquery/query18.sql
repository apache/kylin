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

-- related issue https://github.com/Kyligence/KAP/issues/4286#issuecomment-377104695


select test_kylin_fact.lstg_format_name, sum(test_kylin_fact.price) as GMV 
 , count(*) as TRANS_CNT
 from  

 test_kylin_fact

-- inner JOIN test_category_groupings
-- ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id 

 inner JOIN (select leaf_categ_id, site_id,meta_categ_name from test_category_groupings ) yyy
 ON test_kylin_fact.leaf_categ_id = yyy.leaf_categ_id AND test_kylin_fact.lstg_site_id = yyy.site_id 


 inner JOIN (select cal_dt,week_beg_dt from edw.test_cal_dt  where week_beg_dt >= DATE '2010-02-10'  ) xxx
 ON test_kylin_fact.cal_dt = xxx.cal_dt 
 
 
 where yyy.meta_categ_name > ''
 group by test_kylin_fact.lstg_format_name


