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


SELECT  ORDER_ID,CASE substr(initcapb(LSTG_FORMAT_NAME),1,1)
                WHEN 'A'     THEN 'LSTG_FORMAT_NAME begin with A'
                WHEN 'B'     THEN 'LSTG_FORMAT_NAME begin with B'
                WHEN 'C'     THEN 'LSTG_FORMAT_NAME begin with C'
                ELSE 'LSTG_FORMAT_NAME begin with x' END
FROM    TEST_KYLIN_FACT
order by ORDER_ID
