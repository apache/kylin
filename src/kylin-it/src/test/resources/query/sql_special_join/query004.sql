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

select test_category_groupings.meta_categ_name,
sum(price) as sum_price
from test_kylin_fact as test_kylin_fact
inner join  (select leaf_categ_id,site_id,meta_categ_name from test_category_groupings ) as test_category_groupings
on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id and
test_kylin_fact.lstg_site_id = test_category_groupings.site_id
group by test_category_groupings.meta_categ_name
