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
from (select date_format(to_timestamp('2023-06-08 23:24:25.123'), 'y')    as y,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'yy')   as yy,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'yyy')  as yyy,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'yyyy') as yyyy,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'M')    as M_Up,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'MM')   as MM_Up,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'dd')   as dd,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'd')    as d,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'HH')   as HH_Up,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'H')    as H_Up,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'hh')   as hh_lower,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'h')    as h_lower,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'mm')   as mm_lower,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'm')    as m_lower,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'ss')   as ss_lower,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 's')    as s_lower,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'SSS')  as SSS_Up,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'SS')   as SS_Up,
             date_format(to_timestamp('2023-06-08 23:24:25.123'), 'S')    as S_Up,
             to_timestamp('2023-06-08 23:24:25.123')                      as val) t0
union all
select *
from (select date_format(timestamp '1002-12-01 01:02:03.004', 'y')    as y,
             date_format(timestamp '1002-12-01 01:02:03.004', 'yy')   as yy,
             date_format(timestamp '1002-12-01 01:02:03.004', 'yyy')  as yyy,
             date_format(timestamp '1002-12-01 01:02:03.004', 'yyyy') as yyyy,
             date_format(timestamp '1002-12-01 01:02:03.004', 'M')    as M_Up,
             date_format(timestamp '1002-12-01 01:02:03.004', 'MM')   as MM_Up,
             date_format(timestamp '1002-12-01 01:02:03.004', 'dd')   as dd,
             date_format(timestamp '1002-12-01 01:02:03.004', 'd')    as d,
             date_format(timestamp '1002-12-01 01:02:03.004', 'HH')   as HH_Up,
             date_format(timestamp '1002-12-01 01:02:03.004', 'H')    as H_Up,
             date_format(timestamp '1002-12-01 01:02:03.004', 'hh')   as hh_lower,
             date_format(timestamp '1002-12-01 01:02:03.004', 'h')    as h_lower,
             date_format(timestamp '1002-12-01 01:02:03.004', 'mm')   as mm_lower,
             date_format(timestamp '1002-12-01 01:02:03.004', 'm')    as m_lower,
             date_format(timestamp '1002-12-01 01:02:03.004', 'ss')   as ss_lower,
             date_format(timestamp '1002-12-01 01:02:03.004', 's')    as s_lower,
             date_format(timestamp '1002-12-01 01:02:03.004', 'SSS')  as SSS_Up,
             date_format(timestamp '1002-12-01 01:02:03.004', 'SS')   as SS_Up,
             date_format(timestamp '1002-12-01 01:02:03.004', 'S')    as S_Up,
             timestamp '1002-12-01 01:02:03.004'                      as val) t2
union all
select *
from (select date_format(timestamp '0991-1-12 11:22:33.045', 'y')    as y,
             date_format(timestamp '0991-1-12 11:22:33.045', 'yy')   as yy,
             date_format(timestamp '0991-1-12 11:22:33.045', 'yyy')  as yyy,
             date_format(timestamp '0991-1-12 11:22:33.045', 'yyyy') as yyyy,
             date_format(timestamp '0991-1-12 11:22:33.045', 'M')    as M_Up,
             date_format(timestamp '0991-1-12 11:22:33.045', 'MM')   as MM_Up,
             date_format(timestamp '0991-1-12 11:22:33.045', 'dd')   as dd,
             date_format(timestamp '0991-1-12 11:22:33.045', 'd')    as d,
             date_format(timestamp '0991-1-12 11:22:33.045', 'HH')   as HH_Up,
             date_format(timestamp '0991-1-12 11:22:33.045', 'H')    as H_Up,
             date_format(timestamp '0991-1-12 11:22:33.045', 'hh')   as hh_lower,
             date_format(timestamp '0991-1-12 11:22:33.045', 'h')    as h_lower,
             date_format(timestamp '0991-1-12 11:22:33.045', 'mm')   as mm_lower,
             date_format(timestamp '0991-1-12 11:22:33.045', 'm')    as m_lower,
             date_format(timestamp '0991-1-12 11:22:33.045', 'ss')   as ss_lower,
             date_format(timestamp '0991-1-12 11:22:33.045', 's')    as s_lower,
             date_format(timestamp '0991-1-12 11:22:33.045', 'SSS')  as SSS_Up,
             date_format(timestamp '0991-1-12 11:22:33.045', 'SS')   as SS_Up,
             date_format(timestamp '0991-1-12 11:22:33.045', 'S')    as S_Up,
             timestamp '0991-1-12 11:22:33.045'                      as val) t3
union all
select *
from (select date_format(timestamp '0091-11-22 22:33:44.000', 'y')    as y,
             date_format(timestamp '0091-11-22 22:33:44.000', 'yy')   as yy,
             date_format(timestamp '0091-11-22 22:33:44.000', 'yyy')  as yyy,
             date_format(timestamp '0091-11-22 22:33:44.000', 'yyyy') as yyyy,
             date_format(timestamp '0091-11-22 22:33:44.000', 'M')    as M_Up,
             date_format(timestamp '0091-11-22 22:33:44.000', 'MM')   as MM_Up,
             date_format(timestamp '0091-11-22 22:33:44.000', 'dd')   as dd,
             date_format(timestamp '0091-11-22 22:33:44.000', 'd')    as d,
             date_format(timestamp '0091-11-22 22:33:44.000', 'HH')   as HH_Up,
             date_format(timestamp '0091-11-22 22:33:44.000', 'H')    as H_Up,
             date_format(timestamp '0091-11-22 22:33:44.000', 'hh')   as hh_lower,
             date_format(timestamp '0091-11-22 22:33:44.000', 'h')    as h_lower,
             date_format(timestamp '0091-11-22 22:33:44.000', 'mm')   as mm_lower,
             date_format(timestamp '0091-11-22 22:33:44.000', 'm')    as m_lower,
             date_format(timestamp '0091-11-22 22:33:44.000', 'ss')   as ss_lower,
             date_format(timestamp '0091-11-22 22:33:44.000', 's')    as s_lower,
             date_format(timestamp '0091-11-22 22:33:44.000', 'SSS')  as SSS_Up,
             date_format(timestamp '0091-11-22 22:33:44.000', 'SS')   as SS_Up,
             date_format(timestamp '0091-11-22 22:33:44.000', 'S')    as S_Up,
             timestamp '0091-11-22 22:33:44.000'                      as val) t4
union all
select *
from (select date_format(timestamp '0001-11-22 13:33:44', 'y')    as y,
             date_format(timestamp '0001-11-22 13:33:44', 'yy')   as yy,
             date_format(timestamp '0001-11-22 13:33:44', 'yyy')  as yyy,
             date_format(timestamp '0001-11-22 13:33:44', 'yyyy') as yyyy,
             date_format(timestamp '0001-11-22 13:33:44', 'M')    as M_Up,
             date_format(timestamp '0001-11-22 13:33:44', 'MM')   as MM_Up,
             date_format(timestamp '0001-11-22 13:33:44', 'dd')   as dd,
             date_format(timestamp '0001-11-22 13:33:44', 'd')    as d,
             date_format(timestamp '0001-11-22 13:33:44', 'HH')   as HH_Up,
             date_format(timestamp '0001-11-22 13:33:44', 'H')    as H_Up,
             date_format(timestamp '0001-11-22 13:33:44', 'hh')   as hh_lower,
             date_format(timestamp '0001-11-22 13:33:44', 'h')    as h_lower,
             date_format(timestamp '0001-11-22 13:33:44', 'mm')   as mm_lower,
             date_format(timestamp '0001-11-22 13:33:44', 'm')    as m_lower,
             date_format(timestamp '0001-11-22 13:33:44', 'ss')   as ss_lower,
             date_format(timestamp '0001-11-22 13:33:44', 's')    as s_lower,
             date_format(timestamp '0001-11-22 13:33:44', 'SSS')  as SSS_Up,
             date_format(timestamp '0001-11-22 13:33:44', 'SS')   as SS_Up,
             date_format(timestamp '0001-11-22 13:33:44', 'S')    as S_Up,
             timestamp '0001-11-22 13:33:44'                      as val
      from TEST_KYLIN_FACT limit 1) t5
