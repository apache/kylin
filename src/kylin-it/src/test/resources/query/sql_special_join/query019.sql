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

-- ISSUE #3106

select
ORDER_ID,
META_CATEG_NAME
from
(
    select ORDER_ID,
    LEAF_CATEG_ID,
    LSTG_SITE_ID
    from TEST_KYLIN_FACT
) as t1 inner join TEST_CATEGORY_GROUPINGS as t2
on t1.LEAF_CATEG_ID = t2.LEAF_CATEG_ID and t1.LSTG_SITE_ID = t2.SITE_ID
