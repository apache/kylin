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

select lstg_format_name
, floor(bigint0)+1.1 bigint0
, cast(bigint0 as decimal)-1.1 bigint1
, cast(double0 as decimal)*1.1 double1, cast(decimal0 as decimal)*1.1 decimal1
, cast(DAT0 as timestamp) + interval '1' day as DAT1
, cast(TIME0 as date) - interval '1' day as TIME1
from
    (    select lstg_format_name
                , cast(2 as bigint) bigint0, cast(2 as double) double0, cast(2 as decimal) decimal0
                , DATE'1971-01-01' DAT0
                , TIMESTAMP'1999-01-01 01:01:01' TIME0
                from test_kylin_fact
                group by lstg_format_name
                union all
                select 'extra', cast(null as bigint),cast(null as double),cast(null as decimal)
                , cast(null as date),cast(null as timestamp)
    ) as tmp
