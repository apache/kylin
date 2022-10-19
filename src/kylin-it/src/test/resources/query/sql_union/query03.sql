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

-- union subquery under join
select  count(*) as cnt,TEST_A.SELLER_ID
FROM TEST_KYLIN_FACT as TEST_A
inner join (

    (
    select sum(PRICE) as x, seller_id,count(*) as y
    from
        TEST_KYLIN_FACT where CAL_DT < DATE '2012-08-01'
    group by seller_id
    )

       union

       (
    select sum(PRICE) as x, seller_id,count(*) as y
        from
            TEST_KYLIN_FACT where CAL_DT > DATE '2012-12-01'
        group by seller_id
       )

) TEST_B
on TEST_A.seller_id = TEST_B.seller_id
group by TEST_A.seller_id
