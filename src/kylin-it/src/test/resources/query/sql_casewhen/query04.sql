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


select SELLER_ID, ORDER_ID, LEAF, TRANS
from
(select *, case when LEAF_CATEG_ID >200 then LEAF_CATEG_ID-200 else LEAF_CATEG_ID end as LEAF,
case when TRANS_ID >200 then TRANS_ID-200 else TRANS_ID end as TRANS
from TEST_KYLIN_FACT)
TEST_KYLIN_FACT
where LEAF +1 <TRANS AND SELLER_ID > 10000100 AND LEAF <2000
