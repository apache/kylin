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
select week('2012-01-01')
,week(cal_dt)
,week(cast(cal_dt as varchar))
,month('2012-01-01')
,month(cal_dt)
,month(cast(cal_dt as varchar))
,quarter('2012-01-01')
,quarter(cal_dt)
,quarter(cast(cal_dt as varchar))
,year('2012-01-01')
,year(cal_dt)
,year(cast(cal_dt as varchar))
,dayofyear('2012-01-01')
,dayofyear(cal_dt)
,dayofyear(cast(cal_dt as varchar))
,dayofmonth('2012-01-01')
,dayofmonth(cal_dt)
,dayofmonth(cast(cal_dt as varchar))
,dayofweek('2012-01-01')
,dayofweek(cal_dt)
,dayofweek(cast(cal_dt as varchar))
,dayofweek('2012-01-01 00:10:00')
,dayofweek(cast(cal_dt as timestamp))
from test_kylin_fact

