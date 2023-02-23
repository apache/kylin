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


SELECT a.TRANS_CODE AS TRANS_CODE,
       a.TRANS_ID AS ID,
       a.PRICE_A
FROM
  (SELECT TRANS_ID AS TRANS_CODE,
          TRANS_ID,
          round(cast(PRICE/10 AS decimal),2) AS PRICE_A
   FROM TEST_KYLIN_FACT
   WHERE substring(cast(TRANS_ID AS char(6))
                   FROM 1
                   FOR 4) =
       (SELECT max(substring(cast(TRANS_ID AS char(6))
                             FROM 1
                             FOR 4))
        FROM TEST_KYLIN_FACT)) a
LEFT JOIN
  (SELECT TRANS_ID
   FROM TEST_KYLIN_FACT) b ON a.TRANS_ID=b.TRANS_ID
ORDER BY a.TRANS_ID;
