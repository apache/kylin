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

select
sum(O_TOTALPRICE + 1),
sum(O_TOTALPRICE + 1),
SUM(CUSTOMER_1.C_ACCTBAL)
FROM
TPCH.ORDERS as ORDERS
INNER JOIN TPCH.CUSTOMER as CUSTOMER
ON ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY
INNER JOIN TPCH.CUSTOMER as CUSTOMER_1
ON ORDERS.O_ORDERKEY=CUSTOMER_1.C_CUSTKEY
where CUSTOMER_1.C_ACCTBAL > 5000 -- derived dim
group by O_ORDERDATE

