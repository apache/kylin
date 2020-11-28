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

-- unionall
select max(ORDER_ID) as ORDER_ID_MAX
from TEST_KYLIN_FACT as TEST_A
where ORDER_ID <> 1
union all
select max(ORDER_ID) as ORDER_ID_MAX
from TEST_KYLIN_FACT as TEST_B
;{"scanRowCount":19974,"scanBytes":0,"scanFiles":2,"cuboidId":[788464,788464]}