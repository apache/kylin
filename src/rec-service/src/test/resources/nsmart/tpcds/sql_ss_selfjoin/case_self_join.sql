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
select  i_item_id,
        avg(case when ss.ss_sales_price > ss1.ss_sales_price then 1 else 0 end) agg4
 from TPCDS_BIN_PARTITIONED_ORC_2.store_sales as ss
 join TPCDS_BIN_PARTITIONED_ORC_2.store_sales ss1 on ss.SS_SOLD_TIME_SK = ss1.SS_SOLD_TIME_SK
 join TPCDS_BIN_PARTITIONED_ORC_2.item on ss.ss_item_sk = item.i_item_sk
 group by i_item_id
 order by i_item_id
 limit 100;
