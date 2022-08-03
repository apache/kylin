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
-- runtime join of non-equi-joins
SELECT *
FROM
	(SELECT MAX(test_kylin_fact.price), test_kylin_fact.cal_dt, test_kylin_fact.order_id
	FROM test_kylin_fact
		LEFT JOIN edw.test_cal_dt ON test_kylin_fact.order_id <= '9825'
	GROUP BY test_kylin_fact.cal_dt, test_kylin_fact.order_id
	ORDER BY test_kylin_fact.cal_dt DESC, test_kylin_fact.order_id DESC) T1
	inner join
	(SELECT MAX(test_kylin_fact.price), test_kylin_fact.cal_dt, test_kylin_fact.order_id, test_cal_dt.YEAR_BEG_DT
	FROM test_kylin_fact
		LEFT JOIN edw.test_cal_dt ON test_kylin_fact.order_id = '9825'
	GROUP BY test_kylin_fact.cal_dt, test_kylin_fact.order_id, test_cal_dt.YEAR_BEG_DT ) T2
	on T1.order_id = T2.order_id
	where T1.order_id in (4752, 9825)