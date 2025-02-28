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
-- SQL q48.sql

select sum (ss_quantity)
 from store_sales
 join store on store.s_store_sk = store_sales.ss_store_sk
 join customer_demographics on customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
 join customer_address on store_sales.ss_addr_sk = customer_address.ca_address_sk
 join date_dim on store_sales.ss_sold_date_sk = date_dim.d_date_sk
 where d_year = 1998
 and
 (
  (
   cd_marital_status = 'M'
   and
   cd_education_status = '4 yr Degree'
   and
   ss_sales_price between 100.00 and 150.00
   )
 or
  (
   cd_marital_status = 'M'
   and
   cd_education_status = '4 yr Degree'
   and
   ss_sales_price between 50.00 and 100.00
  )
 or
 (
   cd_marital_status = 'M'
   and
   cd_education_status = '4 yr Degree'
   and
   ss_sales_price between 150.00 and 200.00
 )
 )
 and
 (
  (
  ca_country = 'United States'
  and
  ca_state in ('KY', 'GA', 'NM')
  and ss_net_profit between 0 and 2000
  )
 or
  (
  ca_country = 'United States'
  and
  ca_state in ('MT', 'OR', 'IN')
  and ss_net_profit between 150 and 3000
  )
 or
  (
  ca_country = 'United States'
  and
  ca_state in ('WI', 'MO', 'WV')
  and ss_net_profit between 50 and 25000
  )
 )
