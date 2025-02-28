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
SELECT
       (
              SELECT '2012-01-01') AS sdate,
       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-01']),
       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-02']),
       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-03'])
FROM   test_kylin_fact
WHERE  cal_dt >= date '2012-01-01'
AND    cal_dt <  date'2012-01-07'
UNION ALL
SELECT
       (
              SELECT '2012-01-02') AS sdate,
       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-02']),
       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-03']),
       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-04'])
FROM   test_kylin_fact
WHERE  cal_dt >= date '2012-01-02'
AND    cal_dt <  date'2012-01-07'
