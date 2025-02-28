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

SELECT dat, SUM(PRI) PRI, count(*) COU from
 (
    SELECT test_kylin_fact.cal_dt AS DAT, PRICE AS PRI, lstg_site_id
    FROM (select cal_dt,PRICE, lstg_site_id, leaf_categ_id from test_kylin_fact) as test_kylin_fact
    left JOIN EDW.TEST_CAL_DT DT
    on DT.cal_dt = test_kylin_fact.cal_dt
    left JOIN test_category_groupings
    ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
  ) as Stap
left join
 (
    select site_id from  edw.test_sites group by site_id
  ) as test_sites
ON Stap.lstg_site_id = test_sites.site_id
GROUP BY Stap.DAT
order by Stap.DAT
