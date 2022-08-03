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

SELECT
"TEST_KYLIN_FACT"."LSTG_FORMAT_NAME"
, REPLACE('Kyligence', 'Kyli', 'Kyliiiiiii')
, REPLACE(LSTG_FORMAT_NAME, 'on', 'Kylin')
, REPLACE('TEST-FP-GTC-TEST', "TEST_KYLIN_FACT"."LSTG_FORMAT_NAME", 'Kylin')
, REPLACE('TEST-FP-GTC-TEST', 'FP-GTC', "TEST_KYLIN_FACT"."LSTG_FORMAT_NAME")
from TEST_KYLIN_FACT
WHERE
1 = 1
group by LSTG_FORMAT_NAME
LIMIT 500
