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

SELECT 
 KYLIN_CATEGORY_GROUPINGS.meta_categ_name
 ,KYLIN_CATEGORY_GROUPINGS.categ_lvl2_name
 ,sum(KYLIN_SALES.price) as GMV
 ,count(*) as trans_cnt 
 FROM KYLIN_SALES
 inner JOIN KYLIN_CATEGORY_GROUPINGS
 ON KYLIN_SALES.leaf_categ_id = KYLIN_CATEGORY_GROUPINGS.leaf_categ_id
 AND KYLIN_SALES.lstg_site_id = KYLIN_CATEGORY_GROUPINGS.site_id
 group by 
 KYLIN_CATEGORY_GROUPINGS.meta_categ_name
 ,KYLIN_CATEGORY_GROUPINGS.categ_lvl2_name