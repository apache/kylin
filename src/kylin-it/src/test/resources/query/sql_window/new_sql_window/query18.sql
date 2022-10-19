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

select * from (
          SELECT
          price,
          count(price) over(order by price) as cnt,
          sum(price) over(order by price) as sm,
          row_number() over(order by price) AS rn,
           seller_id
          FROM
        (
            SELECT
              seller_id, sum(price + seller_id) over(order by seller_id) as price
            FROM
              TEST_KYLIN_FACT
            group by price, seller_id
            ORDER BY seller_id
        ) AS T
        )
        where cnt > 100 AND sm > 100 AND rn BETWEEN 1 AND 100
