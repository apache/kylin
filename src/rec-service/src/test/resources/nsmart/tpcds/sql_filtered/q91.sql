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
-- SQL q91.sql
select
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
from catalog_returns
join call_center on catalog_returns.cr_call_center_sk = call_center.cc_call_center_sk
join date_dim on catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
join customer on catalog_returns.cr_returning_customer_sk= customer.c_customer_sk
join customer_address on customer_address.ca_address_sk = customer.c_current_addr_sk
join customer_demographics on customer_demographics.cd_demo_sk = customer.c_current_cdemo_sk
join household_demographics on household_demographics.hd_demo_sk = customer.c_current_hdemo_sk
where   d_year                  = 1999
and     d_moy                   = 11
and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
and     hd_buy_potential like '0-500%'
and     ca_gmt_offset           = -7
group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
order by Returns_Loss desc
