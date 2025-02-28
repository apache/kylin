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
-- SQL q56.sql
with ss as (
  select i_item_id,sum(ss_ext_sales_price) total_sales
  from store_sales
  join date_dim on ss_sold_date_sk = d_date_sk
  join customer_address on ss_addr_sk = ca_address_sk
  join item on ss_item_sk = i_item_sk
  where item.i_item_id in (
    select i.i_item_id
    from item i
    where i_color in ('purple','burlywood','indian')
  )
  and     d_year                  = 2001
  and     d_moy                   = 1
  and     ca_gmt_offset           = -6
  group by i_item_id
),
cs as (
  select i_item_id,sum(cs_ext_sales_price) total_sales
  from catalog_sales
  join date_dim on cs_sold_date_sk = d_date_sk
  join customer_address on cs_bill_addr_sk = ca_address_sk
  join item on cs_item_sk = i_item_sk
  where item.i_item_id in (
    select i.i_item_id
    from item i
    where i_color in ('purple','burlywood','indian')
  )
  and     d_year                  = 2001
  and     d_moy                   = 1
  and     ca_gmt_offset           = -6
  group by i_item_id
),
ws as (
  select i_item_id,sum(ws_ext_sales_price) total_sales
  from web_sales
  join date_dim on ws_sold_date_sk = d_date_sk
  join customer_address on ws_bill_addr_sk = ca_address_sk
  join item on ws_item_sk = i_item_sk
  where item.i_item_id in (
    select i.i_item_id
    from item i
    where i_color in ('purple','burlywood','indian')
  )
  and     d_year                  = 2001
  and     d_moy                   = 1
  and     ca_gmt_offset           = -6
  group by i_item_id
)
select  i_item_id ,sum(total_sales) total_sales
from (
    select * from ss
    union all
    select * from cs
    union all
    select * from ws
    ) tmp1
group by i_item_id
order by total_sales
limit 100
