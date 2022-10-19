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


SELECT COUNT(1)
FROM TEST_KYLIN_FACT
INNER JOIN TEST_ORDER AS TEST_ORDER
ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID
WHERE CAL_DT > '1992-01-01'
AND (
SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1) = 'A'
OR SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1) = 'B'
OR SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1) = 'C'
OR SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1) = 'D'
OR SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1) = 'E'
OR SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1) = 'F'
OR 'G' = SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1)
OR SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1) = 'H'
)
GROUP BY CAL_DT
