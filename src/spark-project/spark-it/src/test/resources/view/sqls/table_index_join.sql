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

--expected_model_hit=orders_model,orders_model;
select
sum(o1.O_TOTALPRICE),
sum(o2.O_TOTALPRICE)
from TPCH.orders_model o1 inner join tpch.orders_model o2
on o1.O_ORDERKEY = o2.O_ORDERKEY
where
o1.O_ORDERSTATUS = 'P'
group by o1.O_ORDERDATE
