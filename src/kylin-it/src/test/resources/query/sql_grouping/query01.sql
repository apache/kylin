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

select
(case grouping(cal_dt) when 1 then 'ALL' else cast(cal_dt as varchar(256)) end) as dt,
(case grouping(slr_segment_cd) when 1 then 'ALL' else cast(slr_segment_cd as varchar(256)) end) as cd,
(case grouping(lstg_format_name) when 1 then 'ALL' else lstg_format_name end) as name,
sum(price) as GMV,
count(*) as TRANS_CNT from test_kylin_fact
where cal_dt between '2012-01-01' and '2012-02-01'
group by rollup(lstg_format_name, cal_dt, slr_segment_cd)
