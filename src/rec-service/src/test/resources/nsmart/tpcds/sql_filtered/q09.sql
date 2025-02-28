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
-- SQL q09.sql
select bucket1, bucket2, bucket3, bucket4, bucket5
from
(select case when count1 > 62316685 then then1 else else1 end bucket1
from (
select count(*) count1, avg(ss_ext_sales_price) then1, avg(ss_net_paid_inc_tax) else1
from store_sales
where ss_quantity between 1 and 20
) A1) B1
CROSS JOIN
(select case when count2 > 19045798 then then2 else else2 end bucket2
from (
select count(*) count2, avg(ss_ext_sales_price) then2, avg(ss_net_paid_inc_tax) else2
from store_sales
where ss_quantity between 21 and 40
) A2) B2
CROSS JOIN
(select case when count3 > 365541424 then then3 else else3 end bucket3
from (
select count(*) count3, avg(ss_ext_sales_price) then3, avg(ss_net_paid_inc_tax) else3
from store_sales
where ss_quantity between 41 and 60
) A3) B3
CROSS JOIN
(select case when count4 > 216357808 then then4 else else4 end bucket4
from (
select count(*) count4, avg(ss_ext_sales_price) then4, avg(ss_net_paid_inc_tax) else4
from store_sales
where ss_quantity between 61 and 80
) A4) B4
CROSS JOIN
(select case when count5 > 184483884 then then5 else else5 end bucket5
from (
select count(*) count5, avg(ss_ext_sales_price) then5, avg(ss_net_paid_inc_tax) else5
from store_sales
where ss_quantity between 81 and 100
) A5) B5
CROSS JOIN
reason
where r_reason_sk = 1
