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
datetime0,
TIMESTAMPADD(YEAR, 6, datetime0) YEAR_ADD,
TIMESTAMPADD(MONTH, 6, datetime0) MONTH_ADD,
TIMESTAMPADD(QUARTER, 6, datetime0) QUARTER_ADD,
TIMESTAMPADD(SECOND, 6, datetime0) SECOND_ADD,
TIMESTAMPADD(MINUTE, 6, datetime0) MINUTE_ADD,
TIMESTAMPADD(HOUR, 6, datetime0) HOUR_ADD,
TIMESTAMPADD(DAY, 6, datetime0) DAY_ADD,
TIMESTAMPADD(WEEK, 6, datetime0) WEEK_ADD,
TIMESTAMPADD(YEAR, -6, datetime0) YEAR_MIN,
TIMESTAMPADD(MONTH, -6, datetime0) MONTH_MIN,
TIMESTAMPADD(QUARTER, -6, datetime0) QUARTER_MIN,
TIMESTAMPADD(SECOND, -6, datetime0) SECOND_MIN,
TIMESTAMPADD(MINUTE, -6, datetime0) MINUTE_MIN,
TIMESTAMPADD(HOUR, -6, datetime0) HOUR_MIN,
TIMESTAMPADD(DAY, -6, datetime0) DAY_MIN,
TIMESTAMPADD(WEEK, -6, datetime0) WEEK_MIN
from TDVT.CALCS
ORDER BY datetime0
limit 2
