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


SELECT SUM(t.a1 + t.a2) AS TWO_COL_PLUS_SUM,
       SUM(t.a1 + t.a2 + 100) AS TWO_COL_PLUS_CONST_SUM,
       SUM(1000000 - t.a1 - t.a2) AS CONST_MINUS_COL_SUM,
       SUM(1000000.0 - t.a1 + t.a2) AS CONST_MINUS_COL_PLUS_COL_SUM
FROM (SELECT SUM(PRICE) AS a1, SUM(ITEM_COUNT) AS a2
    FROM "DEFAULT".TEST_KYLIN_FACT AS TEST_KYLIN_FACT
    GROUP BY CAL_DT
) AS t

