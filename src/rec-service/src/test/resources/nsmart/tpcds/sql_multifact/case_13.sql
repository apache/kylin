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
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales
 join store on store.s_store_sk = store_sales.ss_store_sk
 join customer_demographics on customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
 join household_demographics on store_sales.ss_hdemo_sk=household_demographics.hd_demo_sk
 join customer_address on store_sales.ss_addr_sk = customer_address.ca_address_sk
 join date_dim on store_sales.ss_sold_date_sk = date_dim.d_date_sk
 where date_dim.d_year = 2001
 and((customer_demographics.cd_marital_status = 'M'
  and customer_demographics.cd_education_status = '4 yr Degree'
  and store_sales.ss_sales_price between 100.00 and 150.00
  and household_demographics.hd_dep_count = 3
     )or
     (customer_demographics.cd_marital_status = 'D'
  and customer_demographics.cd_education_status = 'Primary'
  and store_sales.ss_sales_price between 50.00 and 100.00
  and household_demographics.hd_dep_count = 1
     ) or
     (customer_demographics.cd_marital_status = 'U'
  and customer_demographics.cd_education_status = 'Advanced Degree'
  and store_sales.ss_sales_price between 150.00 and 200.00
  and household_demographics.hd_dep_count = 1
     ))
 and((customer_address.ca_country = 'United States'
  and customer_address.ca_state in ('KY', 'GA', 'NM')
  and store_sales.ss_net_profit between 100 and 200
     ) or
     (customer_address.ca_country = 'United States'
  and customer_address.ca_state in ('MT', 'OR', 'IN')
  and store_sales.ss_net_profit between 150 and 300
     ) or
     (customer_address.ca_country = 'United States'
  and customer_address.ca_state in ('WI', 'MO', 'WV')
  and store_sales.ss_net_profit between 50 and 250
     ))
;
