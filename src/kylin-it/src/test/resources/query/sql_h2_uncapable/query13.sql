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

select CAL_DT,
count('aaa') COUNT0, max('aaa') MAX0,
count('aaa') over() COUNT1, max('aaa') over() MAX1, first_value('fff') over() FFF0,
TIMESTAMPDIFF(MONTH, DATE'2012-01-01',test_kylin_fact.cal_dt) * count(1) DIFF1,
TIMESTAMPDIFF(MONTH, TIMESTAMP'2012-01-01 10:01:01',test_kylin_fact.cal_dt) * count(1) over() DIFF2,
max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP'1970-01-01 10:01:01')) MAXTIME,
max(TIMESTAMPADD(SQL_TSI_DAY, 1, DATE'1970-01-01')) MAXDATE,
max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP'1970-01-01 10:01:01')) over() MAXTIME1,
max(TIMESTAMPADD(SQL_TSI_DAY, 1, DATE'1970-01-01')) over() MAXDATE1
from TEST_KYLIN_FACT
where CAL_DT > DATE'2013-12-30'
group by CAL_DT
order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP'1970-01-01 10:01:01'),
TIMESTAMPADD(SQL_TSI_DAY,1, DATE'1970-01-01')
