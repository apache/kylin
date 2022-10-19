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
  f.lstg_format_name
  ,sum(price) as sum_price
FROM
  test_kylin_fact f
  inner join
  (
    select
      lstg_format_name,
      min(slr_segment_cd) as min_seg
    from
      test_kylin_fact
    group by
      lstg_format_name
  ) t on f.lstg_format_name = t.lstg_format_name
where
  f.slr_segment_cd = min_seg
group by
  f.lstg_format_name
