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


select sum(case when time1 = '2012-11-23' then 1
                when time1 >= '' then 2
                when time1 > '2012-11-23' then 3
                when time1 < 'A' then 4
                when time1 <= '2012-11-23' then 5
           end),
       sum(case when time2 = '2012-11-23' then 1
                when time2 >= '' then 2
                when time2 > '2012-11-23' then 3
                when time2 < 'A' then 4
                when time2 <= '2012-11-23' then 5
           end)
from TEST_MEASURE
