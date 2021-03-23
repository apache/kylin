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
select  count(*) as cnt,TEST_A.LSTG_FORMAT_NAME
FROM TEST_KYLIN_FACT as TEST_A
inner join (
    (
        select LSTG_FORMAT_NAME,LSTG_SITE_ID,SLR_SEGMENT_CD,
               sum(price) as GMV, count(1) as TRANS_CNT,
               min(price) as minP, max(price) as maxP,
               COUNT(DISTINCT TEST_COUNT_DISTINCT_BITMAP), COUNT(DISTINCT SELLER_ID)
        from test_kylin_fact
        group by LSTG_FORMAT_NAME,LSTG_SITE_ID,SLR_SEGMENT_CD
    )
    union
    (
        select LSTG_FORMAT_NAME,LSTG_SITE_ID,SLR_SEGMENT_CD,
               sum(price) as GMV, count(1) as TRANS_CNT,
               min(price) as minP, max(price) as maxP,
               COUNT(DISTINCT TEST_COUNT_DISTINCT_BITMAP), COUNT(DISTINCT SELLER_ID)
        from test_kylin_fact
        group by LSTG_FORMAT_NAME,LSTG_SITE_ID,SLR_SEGMENT_CD
     )
) TEST_B
on TEST_A.LSTG_FORMAT_NAME = TEST_B.LSTG_FORMAT_NAME
group by TEST_A.LSTG_FORMAT_NAME
;{"scanRowCount":900,"scanBytes":0,"scanFiles":3,"cuboidId":[14336,14336,14336],"exactlyMatched":[false,false,true,true]}