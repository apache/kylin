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
    t1.leaf_categ_id, t1.CNT1, t2.CNT2
FROM
    (SELECT
        f.leaf_categ_id, count(*) as CNT1
    FROM
        test_kylin_fact f inner join TEST_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id
    WHERE
        f.lstg_format_name = 'ABIN'
    group by f.leaf_categ_id
        ) t1
    INNER JOIN
    (SELECT
        leaf_categ_id, count(*) as CNT2
    FROM
        test_kylin_fact f
    INNER JOIN test_order o ON f.order_id = o.order_id
    WHERE
        buyer_id > 100
    group by leaf_categ_id
    ) t2 ON t1.leaf_categ_id = t2.leaf_categ_id
order by t1.leaf_categ_id, t1.CNT1, t2.CNT2
limit 100
