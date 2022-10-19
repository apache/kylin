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

SELECT LSTG_FORMAT_NAME, sum_price, sum_price2
 FROM (
 	SELECT LSTG_FORMAT_NAME, SUM(price) AS sum_price
 	FROM test_kylin_fact
 	GROUP BY LSTG_FORMAT_NAME
 	UNION ALL
 	SELECT LSTG_FORMAT_NAME, SUM(price) AS sum_price
 	FROM test_kylin_fact
 	GROUP BY LSTG_FORMAT_NAME
 )
 	CROSS JOIN (
 		SELECT SUM(price) AS sum_price2
 		FROM test_kylin_fact
 		GROUP BY LSTG_FORMAT_NAME
 	)
 UNION ALL
 SELECT 'name' AS LSTG_FORMAT_NAME, 11.2 AS sum_price, 22.1 AS sum_price2
