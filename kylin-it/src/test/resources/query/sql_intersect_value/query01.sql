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
select CAL_DT,
intersect_value(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array['2012-01-01']) as first_day,
intersect_value(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array['2012-01-02']) as second_day,
intersect_value(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array['2012-01-03']) as third_day,
intersect_value(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array['2012-01-01','2012-01-02']) as retention_oneday,
intersect_value(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array['2012-01-01','2012-01-02','2012-01-03']) as retention_twoday
from test_kylin_fact
where CAL_DT in ('2012-01-01','2012-01-02','2012-01-03')
group by CAL_DT
;{"scanRowCount":731,"scanBytes":0,"scanFiles":1,"cuboidId":[262144]}