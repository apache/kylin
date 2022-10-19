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

SELECT
  (
    CASE
      WHEN ("t0"."X_measure__0" < 164000) THEN '[0,164000)'
      WHEN ("t0"."X_measure__0" < 166000) THEN '[164000, 166000)'
      ELSE '[166000, +∞'
    END
  ) AS "Calculation_1162491713472385025",
  COUNT("TEST_KYLIN_FACT"."PRICE") AS "usr_GMV_COUNT_ok"
FROM
  "DEFAULT"."TEST_KYLIN_FACT" "TEST_KYLIN_FACT"
  INNER JOIN (
    SELECT
      "TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" AS "LSTG_FORMAT_NAME",
      SUM("TEST_KYLIN_FACT"."PRICE") AS "X_measure__0"
    FROM
      "DEFAULT"."TEST_KYLIN_FACT" "TEST_KYLIN_FACT"
    WHERE
      ("TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" NOT IN ('Auction'))
    GROUP BY
      "TEST_KYLIN_FACT"."LSTG_FORMAT_NAME"
  ) "t0" ON (
    ("TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" = "t0"."LSTG_FORMAT_NAME")
  )
WHERE
  ("TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" NOT IN ('Auction'))
GROUP BY
  (
    CASE
      WHEN ("t0"."X_measure__0" < 164000) THEN '[0,164000)'
      WHEN ("t0"."X_measure__0" < 166000) THEN '[164000, 166000)'
      ELSE '[166000, +∞'
    END
  )
