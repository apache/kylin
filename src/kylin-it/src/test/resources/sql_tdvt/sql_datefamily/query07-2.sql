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
-- with subquery

with ca as(select time0 as t0, time1, datetime0 from tdvt.calcs),
     sta as( select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0 from ca group by ca.datetime0 ),
     da as (select sum(timestampdiff(second, time0, time1)) as col1, datetime0, num1 from tdvt.calcs group by datetime0, num1)
select col1, datetime0 from (
    select da.col1, da.num1, sta.datetime0
    from da left join sta on da.datetime0 = sta.datetime0
    group by sta.datetime0, num1, col1
    order by sta.datetime0
)
group by col1, datetime0
