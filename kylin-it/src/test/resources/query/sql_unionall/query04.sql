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

select CAL_DT,SELLER_ID,lstg_format_name,sum(c1) c1,sum(c2) c2
from
(select CAL_DT,SELLER_ID,lstg_format_name,count(*) c1,0 c2
from TEST_KYLIN_FACT
where CAL_DT='2012-01-01'
group by CAL_DT,SELLER_ID,lstg_format_name
union all
select CAL_DT,SELLER_ID,lstg_format_name,0 c1,count(*) c2
from TEST_KYLIN_FACT
where CAL_DT='2012-01-01'
group by CAL_DT,SELLER_ID,lstg_format_name)
group by CAL_DT,SELLER_ID,lstg_format_name
;{"scanRowCount":20000,"scanBytes":0,"scanFiles":2,"cuboidId":[2097151,2097151]}