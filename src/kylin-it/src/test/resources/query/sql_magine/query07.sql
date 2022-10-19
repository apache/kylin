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


-- ISSUE #3269

SELECT "t2"."X_measure__1" AS "X_measure__1",
       "t1"."X_measure__3" AS "X_measure__3",
       "t4"."X_measure__5" AS "X_measure__5"
FROM
  (SELECT round(AVG("t0"."X_measure__2"), 0) AS "X_measure__3"
   FROM
     (SELECT SELLER_ID,
             round(SUM(PRICE), 0) AS "X_measure__2"
      FROM TEST_KYLIN_FACT
      GROUP BY SELLER_ID) "t0" WHERE "t0"."X_measure__2" > 0 HAVING (COUNT(1) > 0)) "t1"
CROSS JOIN
  (SELECT round(SUM(PRICE), 0) AS "X_measure__1"
   FROM TEST_KYLIN_FACT HAVING (COUNT(1) > 0)) "t2"
CROSS JOIN
  (SELECT round(AVG("t3"."X_measure__4"), 0) AS "X_measure__5"
   FROM
     (SELECT SELLER_ID,
             round(SUM(PRICE), 0) AS "X_measure__4"
      FROM TEST_KYLIN_FACT
      GROUP BY SELLER_ID) "t3" WHERE "t3"."X_measure__4" > 0 HAVING (COUNT(1) > 0)) "t4"
