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
        avg(ss.ss_quantity) agg1,
        avg(ss.ss_list_price) agg2,
        avg(ss.ss_coupon_amt) agg3,
        avg(ss.ss_sales_price) agg4
 from TPCDS_BIN_PARTITIONED_ORC_2.store_sales as ss
 join TPCDS_BIN_PARTITIONED_ORC_2.customer_demographics on ss.ss_cdemo_sk = customer_demographics.cd_demo_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.date_dim on ss.ss_sold_date_sk = date_dim.d_date_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.item on ss.ss_item_sk = item.i_item_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.promotion on ss.ss_promo_sk = promotion.p_promo_sk
 where cd_gender = 'F' and
       cd_marital_status = 'W' and
       cd_education_status = 'Primary' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 1998
 group by i_item_id
 order by i_item_id
 limit 100;
