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


select TEST_CAL_DT.WEEK_BEG_DT
,count(1) COU
from
test_kylin_fact
left join
(
 select WEEK_BEG_DT from EDW.TEST_CAL_DT
 where TEST_CAL_DT.WEEK_BEG_DT >= date'2013-01-01'
 group by TEST_CAL_DT.WEEK_BEG_DT
) TEST_CAL_DT
on test_kylin_fact.cal_dt = TEST_CAL_DT.WEEK_BEG_DT
group by TEST_CAL_DT.WEEK_BEG_DT
