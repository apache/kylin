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

WITH AAA AS
(
SELECT
 LSTG_FORMAT_NAME "针梭织__0",
 COUNT(DISTINCT CASE WHEN TRANS_ID = 100 THEN LSTG_FORMAT_NAME ELSE NULL END) "封样合格率（款）__MAX__1",
 SUM(TRANS_ID) "实际收货配套累计量__SUM__2",
 SUM(CASE WHEN ORDER_ID > 200 AND ORDER_ID > 200 THEN 0 ELSE TRANS_ID END),
 0 "__GROUPING_ID"
 FROM
(
 SELECT
LSTG_FORMAT_NAME
,ORDER_ID
,TRANS_ID
,CAL_DT
 FROM
TEST_KYLIN_FACT
) TT
 WHERE TT.LSTG_FORMAT_NAME != '1'
 GROUP BY LSTG_FORMAT_NAME
 )
,
CCC
AS
(
SELECT
 LSTG_FORMAT_NAME "针梭织__0",
 COUNT(DISTINCT CASE WHEN TRANS_ID = 100 THEN LSTG_FORMAT_NAME ELSE NULL END) "封样合格率（款）__MAX__1",
 SUM(TRANS_ID) "实际收货配套累计量__SUM__2",
 SUM(CASE WHEN ORDER_ID > 200 AND ORDER_ID > 200 THEN 0 ELSE TRANS_ID END),
 0 "__GROUPING_ID"
 FROM
(
 SELECT
LSTG_FORMAT_NAME
,ORDER_ID
,TRANS_ID
,CAL_DT
 FROM
TEST_KYLIN_FACT
) TT
 WHERE TT.LSTG_FORMAT_NAME != '1'
 GROUP BY LSTG_FORMAT_NAME
)

SELECT AAA. * FROM AAA UNION ALL SELECT CCC. * FROM CCC
LIMIT 500
