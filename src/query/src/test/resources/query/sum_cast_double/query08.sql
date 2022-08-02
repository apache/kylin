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



SELECT
    SUM(CAST(SELLER_ID AS DOUBLE))
    ,SUM(CAST(PRICE+0.1 AS DOUBLE))
    ,SUM(CAST(PRICE AS DOUBLE))
    ,SUM(CAST(ORDER_ID+1 AS DOUBLE))
    ,SUM(CAST(ORDER_ID AS DOUBLE))
    ,SUM(CAST(SLR_SEGMENT_CD AS DOUBLE))
    ,LSTG_FORMAT_NAME
    ,TRANS_ID
    ,CAST(PRICE AS DOUBLE) PRICE_D
    ,CAST(ORDER_ID AS DOUBLE) ORDER_ID_D
    ,CAST(ORDER_ID AS DOUBLE) ORDER_ID_D2
FROM "TEST_KYLIN_FACT" AS "TEST_KYLIN_FACT"
GROUP BY LSTG_FORMAT_NAME
         ,TRANS_ID
         ,CAST(PRICE AS DOUBLE)
         ,CAST(ORDER_ID AS DOUBLE)
         ,CAST(ORDER_ID AS DOUBLE)
ORDER BY LSTG_FORMAT_NAME DESC
LIMIT 100



