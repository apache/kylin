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

select t1.LO_ORDERDATE, t1.LO_ORDERKEY, t2.LO_CUSTKEY, t1.RN_1, t2.RN_2
from
(SELECT *, ROW_NUMBER() OVER (PARTITION BY LO_ORDERDATE ORDER BY LO_QUANTITY) AS RN_1
 FROM SSB.LINEORDER
where LO_PARTKEY<1000) t1
left join
(SELECT *, ROW_NUMBER() OVER (PARTITION BY LO_ORDERDATE ORDER BY LO_QUANTITY) AS RN_2
 FROM SSB.LINEORDER
where LO_PARTKEY>2000) t2
on t1.LO_ORDERKEY=t2.LO_ORDERKEY