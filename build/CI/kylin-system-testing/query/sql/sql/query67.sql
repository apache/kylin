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

SELECT (CASE "KYLIN_SALES"."LSTG_FORMAT_NAME" WHEN 'Auction' THEN '111' ELSE '222' END) AS "LSTG_FORMAT_NAME__group_",
  SUM("KYLIN_SALES"."PRICE") AS "sum_PRICE_ok"
FROM "KYLIN_SALES" "KYLIN_SALES"
group by (CASE "KYLIN_SALES"."LSTG_FORMAT_NAME" WHEN 'Auction' THEN '111' ELSE '222' END)  ORDER BY 1 ASC