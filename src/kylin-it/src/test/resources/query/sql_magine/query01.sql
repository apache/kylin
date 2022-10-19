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


-- ISSUE #4286

select sum(t1.PRICE) as c1
from TEST_KYLIN_FACT t1
inner join (
  select t1.LSTG_FORMAT_NAME, sum(t1.PRICE) as x_m
  from TEST_KYLIN_FACT t1
  group by t1.LSTG_FORMAT_NAME
) t2
on (t1.LSTG_FORMAT_NAME= t2.LSTG_FORMAT_NAME)
where t2.x_m >= 0.001
