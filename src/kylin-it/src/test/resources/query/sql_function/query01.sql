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


-- test spader function
-- from KE query/sql_sparder_function/query01.sql
-- issue #6152 commit 0bb8203

SELECT CAST(MOD(ITEM_COUNT, 2) AS BIGINT) AS "mod", LN(ITEM_COUNT) AS "ln"
	, round(LOG10(ITEM_COUNT), 6) AS "log10"
	, EXP(ITEM_COUNT) AS "exp", ACOS(ITEM_COUNT) AS "acos"
	, ASIN(ITEM_COUNT) AS "asin", ATAN(ITEM_COUNT) AS "atan"
	, ATAN2(ITEM_COUNT, 0.8) AS "atan2"
	, COS(ITEM_COUNT) AS "cos", DEGREES(ITEM_COUNT) AS "degrees"
	, RADIANS(ITEM_COUNT) AS "radians", SIGN(ITEM_COUNT) AS "sign"
	, TAN(ITEM_COUNT) AS "tan", SIN(ITEM_COUNT) AS "sin"
FROM TEST_KYLIN_FACT
where ITEM_COUNT is not null and ITEM_COUNT > 10
GROUP BY ITEM_COUNT
ORDER BY "mod"
