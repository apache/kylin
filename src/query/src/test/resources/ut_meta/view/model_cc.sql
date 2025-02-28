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

SELECT "LINEORDER"."CC3" AS "CC3","LINEORDER"."LO_ORDERKEY" AS "LO_ORDERKEY","LINEORDER"."CC1" AS "CC1","LINEORDER"."CC2" AS "CC2","CUSTOMER"."C_MKTSEGMENT" AS "C_MKTSEGMENT","LINEORDER"."LO_SHIPMODE" AS "LO_SHIPMODE","LINEORDER"."LO_QUANTITY" AS "LO_QUANTITY" FROM "SSB"."LINEORDER" AS "LINEORDER" INNER JOIN "SSB"."CUSTOMER" AS "CUSTOMER" ON "CUSTOMER"."C_CUSTKEY" = "LINEORDER"."LO_CUSTKEY"
