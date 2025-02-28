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

SELECT "SN"."N_NAME", SUM(l_extendedprice) "REVENUE"
FROM TPCH."LINEITEM"
INNER JOIN TPCH."ORDERS" ON "L_ORDERKEY" = "O_ORDERKEY"
INNER JOIN TPCH."CUSTOMER" ON "O_CUSTKEY" = "C_CUSTKEY"
INNER JOIN TPCH."NATION" "CN" ON "C_NATIONKEY" = "CN"."N_NATIONKEY"
INNER JOIN TPCH."SUPPLIER" ON "L_SUPPKEY" = "S_SUPPKEY"
INNER JOIN TPCH."NATION" "SN" ON "S_NATIONKEY" = "SN"."N_NATIONKEY"
INNER JOIN TPCH."REGION" ON "SN"."N_REGIONKEY" = "R_REGIONKEY"
WHERE "R_NAME" = 'A' AND "CN"."N_NAME" = "SN"."N_NAME" AND "O_ORDERDATE" >= '2010-01-01' AND "O_ORDERDATE" < '2010-01-02'
GROUP BY "SN"."N_NAME"
ORDER BY "REVENUE" DESC
