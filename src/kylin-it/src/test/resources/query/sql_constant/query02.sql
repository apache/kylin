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
select
  trunc('2009-02-12', 'MM'),trunc('2015-10-27', 'YEAR'),
  trunc(date'2009-02-12', 'MM'),trunc(timestamp'2009-02-12 00:00:00', 'MM'),
  add_months('2016-08-31', 1),add_months(date'2016-08-31', 2),add_months(timestamp'2016-08-31 00:00:00', 1),
  date_add('2016-07-30', 1),date_add(date'2016-07-30', 1),date_add(timestamp'2016-07-30 00:00:00', 1),
  date_sub('2016-07-30', 1),date_sub(date'2016-07-30', 1),date_sub(timestamp'2016-07-30 00:00:00', 1),
  from_unixtime(0, 'yyyy-MM-dd HH:mm:ss'),
  from_utc_timestamp('2016-08-31', 'Asia/Seoul'),from_utc_timestamp(timestamp'2016-08-31 00:00:00', 'Asia/Seoul'),from_utc_timestamp(date'2016-08-31', 'Asia/Seoul'),
  months_between('1997-02-28 10:30:00', '1996-10-30'),months_between(timestamp'1997-02-28 10:30:00', date'1996-10-30'),
  to_utc_timestamp('2016-08-31', 'Asia/Seoul'),to_utc_timestamp(timestamp'2016-08-31 00:00:00', 'Asia/Seoul'),to_utc_timestamp(date'2016-08-31', 'Asia/Seoul')


