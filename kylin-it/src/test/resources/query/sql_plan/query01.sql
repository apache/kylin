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

SELECT ORDER_ID
FROM test_kylin_fact
GROUP BY CAST(CASE
		WHEN '1030101' = '1030101' THEN substring(COALESCE(LSTG_FORMAT_NAME, '888888888888'), 1, 1)
		WHEN '1030101' = '1030102' THEN substring(COALESCE(LSTG_FORMAT_NAME, '999999999999'), 1, 1)
		WHEN '1030101' = '1030103' THEN substring(COALESCE(LSTG_FORMAT_NAME, '777777777777'), 1, 1)
		WHEN '1030101' = '1030104' THEN substring(COALESCE(LSTG_FORMAT_NAME, '666666666666'), 1, 1)
	END AS varchar(256)), ORDER_ID
;{"scanRowCount":10000,"scanBytes":0,"scanFiles":1,"cuboidId":[2097151]}