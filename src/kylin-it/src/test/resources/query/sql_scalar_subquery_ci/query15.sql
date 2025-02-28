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


select DATES.D_DATEKEY, count(DATES.D_YEAR) count_year, DT.DATE_TIME
from (select 19950329 as DATE_TIME
      union
      select 19950401 as DATE_TIME
      union all
      select 19950329 as DATE_TIME
      union all
      select 19950401 as DATE_TIME
      union all
      select 19950517 as DATE_TIME) DT
         left join SSB.DATES on DT.DATE_TIME = DATES.D_DATEKEY
where DATES.D_MONTH = 'May'
group by dt.date_time, DATES.D_DATEKEY
