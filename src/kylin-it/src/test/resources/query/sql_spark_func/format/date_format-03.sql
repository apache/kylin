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

select *
from (select date_format(to_date('2023-06-08'), 'y')    as y,
             date_format(to_date('2023-06-08'), 'yy')   as yy,
             date_format(to_date('2023-06-08'), 'yyy')  as yyy,
             date_format(to_date('2023-06-08'), 'yyyy') as yyyy,
             date_format(to_date('2023-06-08'), 'M')    as M_Up,
             date_format(to_date('2023-06-08'), 'MM')   as MM_Up,
             date_format(to_date('2023-06-08'), 'dd')   as dd,
             date_format(to_date('2023-06-08'), 'd')    as d,
             to_date('2023-06-08')                      as val) t0
union all
select *
from (select date_format(date '1002-12-01', 'y')    as y,
             date_format(date '1002-12-01', 'yy')   as yy,
             date_format(date '1002-12-01', 'yyy')  as yyy,
             date_format(date '1002-12-01', 'yyyy') as yyyy,
             date_format(date '1002-12-01', 'M')    as M_Up,
             date_format(date '1002-12-01', 'MM')   as MM_Up,
             date_format(date '1002-12-01', 'dd')   as dd,
             date_format(date '1002-12-01', 'd')    as d,
             date '1002-12-01'                      as val) t2
union all
select *
from (select date_format(date '0991-1-12', 'y')    as y,
             date_format(date '0991-1-12', 'yy')   as yy,
             date_format(date '0991-1-12', 'yyy')  as yyy,
             date_format(date '0991-1-12', 'yyyy') as yyyy,
             date_format(date '0991-1-12', 'M')    as M_Up,
             date_format(date '0991-1-12', 'MM')   as MM_Up,
             date_format(date '0991-1-12', 'dd')   as dd,
             date_format(date '0991-1-12', 'd')    as d,
             date '0991-1-12'                      as val) t3
union all
select *
from (select date_format(date '0091-11-22', 'y')    as y,
             date_format(date '0091-11-22', 'yy')   as yy,
             date_format(date '0091-11-22', 'yyy')  as yyy,
             date_format(date '0091-11-22', 'yyyy') as yyyy,
             date_format(date '0091-11-22', 'M')    as M_Up,
             date_format(date '0091-11-22', 'MM')   as MM_Up,
             date_format(date '0091-11-22', 'dd')   as dd,
             date_format(date '0091-11-22', 'd')    as d,
             date '0091-11-22'                      as val) t4
union all
select *
from (select date_format(date '0001-11-22', 'y')    as y,
             date_format(date '0001-11-22', 'yy')   as yy,
             date_format(date '0001-11-22', 'yyy')  as yyy,
             date_format(date '0001-11-22', 'yyyy') as yyyy,
             date_format(date '0001-11-22', 'M')    as M_Up,
             date_format(date '0001-11-22', 'MM')   as MM_Up,
             date_format(date '0001-11-22', 'dd')   as dd,
             date_format(date '0001-11-22', 'd')    as d,
             date '0001-11-22'                      as val
      from TEST_KYLIN_FACT limit 1) t5
