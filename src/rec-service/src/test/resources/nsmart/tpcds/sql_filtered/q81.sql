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
-- SQL q81.sql
with customer_total_return as
 (select cr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state,
    sum(cr_return_amt_inc_tax) as ctr_total_return
 from catalog_returns
 join date_dim on cr_returned_date_sk = d_date_sk
 join customer_address on cr_returning_addr_sk = ca_address_sk
 where d_year = 1998
 group by cr_returning_customer_sk
         ,ca_state ),
 customer_avg_return as
 (select avg(ctr2.ctr_total_return) as avg_ctr, ctr_state
    from customer_total_return ctr2
    group by ctr_state)
 select c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 from customer_total_return ctr1
 join customer on ctr1.ctr_customer_sk = c_customer_sk
 join customer_address on ca_address_sk = c_current_addr_sk
 join customer_avg_return car on ctr1.ctr_state = car.ctr_state
 where ctr1.ctr_total_return > car.avg_ctr *1.2
 and ca_state = 'TN'
 order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 limit 100
