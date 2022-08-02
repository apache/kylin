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

-- joined

SELECT CNT1, CNT2, T1.CAL_DT
FROM
(
SELECT
COUNT(DISTINCT (CASE WHEN CAL_DT = DATE'2012-10-30' AND SELLER_ID > 1 THEN PRICE ELSE NULL END)) CNT1, CAL_DT
FROM "TEST_KYLIN_FACT" AS "TEST_KYLIN_FACT"
GROUP BY CAL_DT
) T1
JOIN
(
SELECT
CAL_DT, COUNT(DISTINCT (CASE WHEN SELLER_ID > 1 THEN PRICE ELSE NULL END)) CNT2
FROM "TEST_KYLIN_FACT" AS "TEST_KYLIN_FACT"
GROUP BY CAL_DT
) T2 ON T1.CAL_DT = T2.CAL_DT
