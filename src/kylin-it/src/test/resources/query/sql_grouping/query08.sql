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

select cal_dt, sum(case when c1 > 100 then 100 else c1 end)
from (
    select
    TEST_KYLIN_FACT.ITEM_COUNT * TEST_KYLIN_FACT.PRICE as c1 ,
    CAL_DT
    from
        TEST_KYLIN_FACT
    where
     CAL_DT  >   date'2013-12-30'
     )
group by grouping sets((c1+123, cal_dt), (cal_dt))
