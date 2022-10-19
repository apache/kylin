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


select case when coalesce(item_count, 0) <=10 and coalesce(price, 0) >= 0.0 then 'a'
            when coalesce(item_count, 0) < 0 then 'exception'
            else null end,
      sum(case when price > 1 and item_count < 10 and seller_id > 20 then 1 else 0 end),
      sum(case when price > 1 or item_count < 5 or seller_id > 10 then price else 0 end)
from test_kylin_fact
group by case when coalesce(item_count, 0) <=10 and coalesce(price, 0) >= 0.0 then 'a'
            when coalesce(item_count, 0) < 0 then 'exception'
            else null end
order by 1, 2, 3
