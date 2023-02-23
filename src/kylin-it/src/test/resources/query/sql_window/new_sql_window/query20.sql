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
lag(cal_dt,50,date'2019-01-01') over (partition by seller_id order by item_count)
,lag(cal_dt,50,'2019-01-01') over (partition by seller_id order by item_count)
,lead(item_count,1,'2019') over (partition by seller_id order by cal_dt)
,lead(item_count,1,2019) over (partition by seller_id order by cal_dt)
,lead(price,1,1.234) over (partition by seller_id order by cal_dt)
from test_kylin_fact
