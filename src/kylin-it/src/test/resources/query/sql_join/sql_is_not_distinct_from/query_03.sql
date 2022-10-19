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

-- KE-19841 optimize non-equal join with is not distinct from condition
SELECT SUM(PRICE),
       CAL_DT
FROM TEST_KYLIN_FACT
LEFT JOIN
  (SELECT TRANS_ID, ORDER_ID
   FROM TEST_KYLIN_FACT
   WHERE TRANS_ID > 100000000 group by TRANS_ID, ORDER_ID) FACT
ON (TEST_KYLIN_FACT.TRANS_ID = FACT.TRANS_ID
or (TEST_KYLIN_FACT.TRANS_ID is null and FACT.TRANS_ID is null))
and TEST_KYLIN_FACT.ORDER_ID = FACT.ORDER_ID
GROUP BY CAL_DT
