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
SELECT D_YEAR, S_CITY, P_BRAND,
SUM(LO_REVENUE - LO_SUPPLYCOST) AS PROFIT
FROM SSB.P_LINEORDER,SSB.DATES, SSB.CUSTOMER, SSB.SUPPLIER, SSB.PART
WHERE  LO_CUSTKEY = C_CUSTKEY
AND LO_SUPPKEY = S_SUPPKEY
AND LO_PARTKEY = P_PARTKEY
AND  LO_ORDERDATE = D_DATEKEY
AND S_NATION = 'UNITED STATES'
AND (D_YEAR = 1997 OR D_YEAR = 1998)
AND P_CATEGORY = 'MFGR#14'
GROUP BY D_YEAR, S_CITY, P_BRAND
ORDER BY D_YEAR, S_CITY, P_BRAND;
