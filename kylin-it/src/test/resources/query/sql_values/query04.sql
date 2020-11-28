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

SELECT *
FROM (
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
) as t1
	CROSS JOIN (
		SELECT MAX(price) AS sum_price_2
		FROM test_kylin_fact
		GROUP BY leaf_categ_id
	) as t2
UNION ALL
SELECT *
FROM (
	SELECT cast(1  as bigint) AS leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	WHERE 1 <> 1
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT cast(2  as bigint) AS leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	WHERE 1 <> 1
	GROUP BY leaf_categ_id
) as t3
	CROSS JOIN (
		SELECT MAX(price) AS sum_price_2
		FROM test_kylin_fact
		GROUP BY leaf_categ_id
	) as t4
UNION ALL
SELECT *
FROM (
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
) as t5
	CROSS JOIN (
		SELECT MAX(price) AS sum_price_2
		FROM test_kylin_fact
		GROUP BY leaf_categ_id
	) as t6
ORDER BY 1
;{"scanRowCount":864,"scanBytes":0,"scanFiles":6,"cuboidId":[245760,245760,245760,245760,245760,245760]}