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


select META_CATEG_NAME,
round(
    avg(
    case when LEAF_CATEG_ID*SITE_ID>4000
    then cast(LEAF_CATEG_ID as decimal(19,4))
    else cast(LEAF_CATEG_ID*SITE_ID as decimal(19,4))
    end
    ),
2)
AVG_PRODUCT,
CAST(SUM(LEAF_CATEG_ID*SITE_ID) as BIGINT) SUM_PRODUCT,
max(cast(LEAF_CATEG_ID*SITE_ID as varchar(40))) MAX_PRODUCT
from TEST_CATEGORY_GROUPINGS
group by META_CATEG_NAME
