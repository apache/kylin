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


SELECT CUSTOMER.C_NAME,
       count(LINEORDER.LO_ORDERKEY) count_order,
       LINEORDER.LO_ORDERDATE,
       dt.DATE_TIME,
       sum(CUSTOMER.C_CUSTKEY)      sum_custkey
FROM SSB.P_LINEORDER as LINEORDER
         INNER JOIN "SSB"."CUSTOMER" as "CUSTOMER"
                    on "LINEORDER"."LO_CUSTKEY" = "CUSTOMER"."C_CUSTKEY"
         inner join (select 19960102 as DATE_TIME) dt
                    on LINEORDER.LO_ORDERDATE = dt.DATE_TIME
group by CUSTOMER.C_NAME, LINEORDER.LO_ORDERDATE, dt.DATE_TIME
