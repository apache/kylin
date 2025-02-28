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


SELECT t1.cal_dt, t1.sum_price,t1.lstg_site_id 
FROM (
  select cal_dt, lstg_site_id, sum(price) as sum_price
  from test_kylin_fact
  group by cal_dt, lstg_site_id
  
) t1

inner JOIN edw.test_cal_dt as test_cal_dt
on t1.cal_dt=test_cal_dt.cal_dt

inner JOIN edw.test_sites as test_sites
on t1.lstg_site_id = test_sites.site_id

