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
-- Unless required by applicable law or agreed to in writing, softwarea
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT Count(DISTINCT CASE WHEN KYLIN_FACT.lstg_format_name = 'Auction'
                           THEN KYLIN_FACT.BUYER_ID
                           ELSE Cast(NULL AS FLOAT) END) AS TEMP_________,
       Count(DISTINCT lstg_format_name)
FROM (SELECT * FROM test_kylin_fact left join test_order ON test_kylin_fact.order_id = test_order.order_id) KYLIN_FACT
WHERE KYLIN_FACT.cal_dt = DATE '2012-01-01'