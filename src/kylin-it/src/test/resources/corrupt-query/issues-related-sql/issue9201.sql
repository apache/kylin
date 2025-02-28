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

-- SELECT APPNAME, APTYPE, SUM(BATTERYGASGAUGE) AS BATTERYGASGAUGE
-- 	, SUM(USETIME) AS USETIME
-- FROM BIAPS.POWER_ANALYSE_DEVANDAPP_KPIVALUE_BAT_VER A
-- WHERE (BATCHID = '20181123032600878'
-- 	AND DEVICENAME = 'MODEM'
-- 	AND 'CYCLE' = 1)
-- GROUP BY APPNAME, APTYPE


select lstg_format_name, trans_id, sum(price) as sum_price, sum(item_count) as sum_count
from test_kylin_fact
where (cal_dt='20120201' and seller_id = 20 and lstg_site_id=10)
group by lstg_format_name, trans_id
