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

SELECT  TEST_CAL_DT.WEEK_BEG_DT AS COL1,
         (CASE WHEN "TEST_CAL_DT"."CAL_DT" <= CAST('2012-12-31' AS DATE) THEN PRICE_SUM WHEN "TEST_CAL_DT"."CAL_DT" > CAST('2012-12-31' AS DATE) THEN 1 ELSE NULL END) AS COL2
FROM (
	    SELECT CAL_DT,
	           SUM(CASE WHEN LSTG_FORMAT_NAME='FP-non GTC' THEN PRICE * 2 ELSE 2 END) AS PRICE_SUM
	    FROM "DEFAULT".TEST_KYLIN_FACT
	    GROUP BY CAL_DT
	  ) AS TEST_KYLIN_FACT

INNER JOIN "EDW"."TEST_CAL_DT" "TEST_CAL_DT" ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT
GROUP BY TEST_CAL_DT.CAL_DT,PRICE_SUM,TEST_CAL_DT.WEEK_BEG_DT
