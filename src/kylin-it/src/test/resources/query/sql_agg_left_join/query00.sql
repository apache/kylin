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

select t2.fcol_7 fcol_7,
    count(distinct t2.fcol_6) fcol_6
from (
        select t1.company_code fcol_6,
            t1.type_name fcol_7,
            case
                when t1.created_date = t0.fcol_1 then 'TRUE'
                else 'FALSE'
            end fcol_10
        from (
                select company_code,
                    created_date,
                    type_name
                from "DEFAULT"."TEST_AGG_PUSH_DOWN"
            ) t1
            join (
                select company_code,
                    max(created_date) fcol_1
                from "DEFAULT"."TEST_AGG_PUSH_DOWN"
                group by company_code
            ) t0 on t1.company_code = t0.company_code
    ) t2
    join (
        select 'TRUE' fcol_17
    ) t5 on (
        t2.fcol_10 = t5.fcol_17
    )
group by t2.fcol_7