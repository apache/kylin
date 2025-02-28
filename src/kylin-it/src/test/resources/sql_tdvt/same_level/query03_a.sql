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

select Sta.datetime0 as DAT, sum(Sta.NUM4) as PRI, count(*) as COU
    from TDVT.CALCS CALCS
    inner join
        (    select datetime0, NUM4
             from TDVT.CALCS CALCS
             cross join
             (select 1)
             as temp
        ) as Sta
    on CALCS.datetime0 = Sta.datetime0
group by Sta.datetime0
order by Sta.datetime0
