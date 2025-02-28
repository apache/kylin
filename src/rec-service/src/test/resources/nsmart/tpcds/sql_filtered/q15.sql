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
-- SQL q15.sql
select  ca_zip
       ,sum(cs_sales_price)
 from catalog_sales
     join customer on catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
     join customer_address on customer.c_current_addr_sk = customer_address.ca_address_sk
     join date_dim on catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 where ( substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
        or customer_address.ca_state in ('CA','WA','GA')
        or catalog_sales.cs_sales_price > 500)
  and date_dim.d_qoy = 2 and date_dim.d_year = 2000
 group by ca_zip
 order by ca_zip
 limit 100
