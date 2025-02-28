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

-- test functions: year, month, quarter, day, hour, minute, second

select count(distinct year(date0)), max(extract(year from date1)),
       count(distinct month(date0)), max(extract(month from date1)),
       count(distinct quarter(date0)), max(extract(quarter from date1)),
       count(distinct hour(date0)), max(extract(hour from date1)),
       count(distinct minute(date0)), max(extract(minute from date1)),
       count(distinct second(date0)), max(extract(second from date1)),
       count(dayofmonth(date0)), max(extract(day from date1)),
       count(distinct hour(cast(date0 as string))), max(extract(hour from cast(date1 as string))),
       count(distinct minute(cast(date0 as string))), max(extract(minute from cast(date1 as string))),
       count(distinct second(cast(date0 as string))), max(extract(second from cast(date1 as string)))
from tdvt.calcs as calcs