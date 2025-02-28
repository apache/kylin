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
-- SQL q25.sql
select
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 ,sum(ss_net_profit) as store_sales_profit
 ,sum(sr_net_loss) as store_returns_loss
 ,sum(cs_net_profit) as catalog_sales_profit
 from
 store_sales
 join store_returns on ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
 join catalog_sales on sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
 join date_dim d1 on d1.d_date_sk = ss_sold_date_sk
 join date_dim d2 on sr_returned_date_sk = d2.d_date_sk
 join date_dim d3 on cs_sold_date_sk = d3.d_date_sk
 join store on s_store_sk = ss_store_sk
 join item on i_item_sk = ss_item_sk
 where d1.d_moy = 4
 and d1.d_year = 1998
 and d2.d_moy               between 4 and  10
 and d2.d_year              = 1998
 and d3.d_moy               between 4 and  10
 and d3.d_year              = 1998
 group by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 order by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 limit 100
