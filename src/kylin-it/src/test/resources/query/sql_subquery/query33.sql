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



  select  (case when '2'='1' then test_category_groupings.USER_DEFINED_FIELD1 when '2'='2' then test_category_groupings.USER_DEFINED_FIELD3 else test_category_groupings.GROUPINGS_CRE_DATE end) as xxx , sum(price) as sum_price
  from test_kylin_fact
  inner JOIN edw.test_cal_dt as test_cal_dt
  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
  inner JOIN test_category_groupings
  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
  inner JOIN edw.test_sites as test_sites
  ON test_kylin_fact.lstg_site_id = test_sites.site_id

  where case when '2'='1' then test_kylin_fact.cal_dt < date'2012-04-01' when '2'='2' then  test_cal_dt.week_beg_dt > date'2012-04-01' else  test_kylin_fact.lstg_site_id is not null end

  group by  (case when '2'='1' then test_category_groupings.USER_DEFINED_FIELD1 when '2'='2' then test_category_groupings.USER_DEFINED_FIELD3 else test_category_groupings.GROUPINGS_CRE_DATE end)

