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

select "SELLER_ID", max("PRICE"), "CAL_DT", count("LSTG_FORMAT_NAME") from "TEST_KYLIN_FACT" inner join "TEST_ORDER" on "TEST_KYLIN_FACT"."CAL_DT" = "TEST_ORDER"."TEST_DATE_ENC" group by "SELLER_ID", "PRICE", "CAL_DT", "LSTG_FORMAT_NAME" union select "SELLER_ID", min("PRICE"), "CAL_DT", count(distinct "LSTG_FORMAT_NAME") from "TEST_KYLIN_FACT" inner join "TEST_ORDER" on "TEST_KYLIN_FACT"."CAL_DT" = "TEST_ORDER"."TEST_DATE_ENC" group by "SELLER_ID", "PRICE", "CAL_DT", "LSTG_FORMAT_NAME" order by "SELLER_ID", "CAL_DT" LIMIT 100
