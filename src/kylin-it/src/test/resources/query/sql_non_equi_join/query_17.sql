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
-- right join
Select max(test_kylin_fact.price),test_kylin_fact.cal_dt,test_kylin_fact.order_id
From test_kylin_fact
Right join edw.test_cal_dt
On test_kylin_fact.order_id>edw.test_cal_dt.AGE_FOR_YEAR_ID
Group by test_kylin_fact.cal_dt,test_kylin_fact.order_id
order by test_kylin_fact.cal_dt desc ,test_kylin_fact.order_id desc ;
