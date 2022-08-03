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
select a.LSTG_FORMAT_NAME,a.LEAF_CATEG_ID,c.SITE_ID
from (
select LSTG_FORMAT_NAME,LEAF_CATEG_ID,LSTG_SITE_ID,count(distinct SELLER_ID) as type_SELLER_ID
from TEST_KYLIN_FACT
where CAL_DT = '2012-01-01'
group by LSTG_FORMAT_NAME,LEAF_CATEG_ID,LSTG_SITE_ID
)a
inner join (
select LSTG_FORMAT_NAME,LEAF_CATEG_ID,LSTG_SITE_ID,count(distinct SELLER_ID) as type_SELLER_ID
from TEST_KYLIN_FACT
where CAL_DT = '2012-01-01'
group by LSTG_FORMAT_NAME,LEAF_CATEG_ID,LSTG_SITE_ID
)b
on a.LSTG_FORMAT_NAME=b.LSTG_FORMAT_NAME
and a.LEAF_CATEG_ID=b.LEAF_CATEG_ID
left join TEST_CATEGORY_GROUPINGS as c
on a.LSTG_SITE_ID=c.SITE_ID